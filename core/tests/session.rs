use std::sync::{Arc, Mutex};

use open_arrow_sink_core::{
    DataWriter, Error, RecordBatch, Result, SchemaRef, SessionCommitter, WriteSession,
    WriteSummary, async_trait,
};

#[derive(Clone, Copy, Debug, PartialEq)]
enum Event {
    Write,
    Finish,
    AbortWriter,
    Publish,
    AbortSession,
}
#[derive(Clone, Copy, Default)]
struct Faults {
    write: bool,
    pending: bool,
    finish: bool,
    publish: bool,
    abort: bool,
}
type Events = Arc<Mutex<Vec<Event>>>;
struct Physical {
    schema: SchemaRef,
    events: Events,
    faults: Faults,
}
struct Committer {
    events: Events,
    faults: Faults,
}

#[async_trait]
impl DataWriter for Physical {
    type Output = u64;
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }
    async fn write(&mut self, _: RecordBatch) -> Result<()> {
        self.events.lock().unwrap().push(Event::Write);
        if self.faults.pending {
            std::future::pending::<()>().await;
        }
        if self.faults.write {
            Err(Error::SchemaMismatch)
        } else {
            Ok(())
        }
    }
    async fn finish(&mut self) -> Result<u64> {
        self.events.lock().unwrap().push(Event::Finish);
        if self.faults.finish {
            Err(Error::SchemaMismatch)
        } else {
            Ok(7)
        }
    }
    async fn abort(self: Box<Self>) -> Result<()> {
        self.events.lock().unwrap().push(Event::AbortWriter);
        if self.faults.abort {
            Err(Error::SchemaMismatch)
        } else {
            Ok(())
        }
    }
}
#[async_trait]
impl SessionCommitter<u64> for Committer {
    async fn commit(self: Box<Self>, outputs: Vec<u64>) -> Result<WriteSummary> {
        self.events.lock().unwrap().push(Event::Publish);
        assert_eq!(outputs, vec![7]);
        if self.faults.publish {
            Err(Error::SchemaMismatch)
        } else {
            Ok(WriteSummary { rows_written: 7 })
        }
    }
    async fn abort(self: Box<Self>) -> Result<()> {
        self.events.lock().unwrap().push(Event::AbortSession);
        Ok(())
    }
}
fn session(faults: Faults, count: usize) -> (WriteSession<u64>, Events) {
    let events = Events::default();
    let schema = Arc::new(arrow_schema::Schema::empty());
    let writers = (0..count)
        .map(|_| {
            Box::new(Physical {
                schema: schema.clone(),
                events: events.clone(),
                faults,
            }) as Box<dyn DataWriter<Output = u64>>
        })
        .collect();
    (
        WriteSession {
            writers,
            committer: Box::new(Committer {
                events: events.clone(),
                faults,
            }),
        },
        events,
    )
}
fn empty_batch() -> RecordBatch {
    RecordBatch::new_empty(Arc::new(arrow_schema::Schema::empty()))
}

#[tokio::test]
async fn success_finishes_before_exactly_one_publication() {
    let (session, events) = session(Faults::default(), 1);
    let mut writer = session.into_writer().await.unwrap();
    writer.write(empty_batch()).await.unwrap();
    assert_eq!(writer.commit().await.unwrap().rows_written, 7);
    assert_eq!(
        *events.lock().unwrap(),
        [Event::Write, Event::Finish, Event::Publish]
    );
}

#[tokio::test]
async fn failed_write_cannot_resume_or_publish() {
    let (session, events) = session(
        Faults {
            write: true,
            ..Default::default()
        },
        1,
    );
    let mut writer = session.into_writer().await.unwrap();
    assert!(writer.write(empty_batch()).await.is_err());
    assert!(writer.write(empty_batch()).await.is_err());
    assert!(writer.commit().await.is_err());
    assert_eq!(
        *events.lock().unwrap(),
        [Event::Write, Event::AbortWriter, Event::AbortSession]
    );
}

#[tokio::test]
async fn cancelled_write_cannot_resume_or_publish() {
    let (session, events) = session(
        Faults {
            pending: true,
            ..Default::default()
        },
        1,
    );
    let mut writer = session.into_writer().await.unwrap();
    let mut pending = writer.write(empty_batch());
    let mut context = std::task::Context::from_waker(std::task::Waker::noop());
    assert!(std::future::Future::poll(pending.as_mut(), &mut context).is_pending());
    drop(pending);
    assert!(writer.write(empty_batch()).await.is_err());
    assert!(writer.commit().await.is_err());
    assert_eq!(
        *events.lock().unwrap(),
        [Event::Write, Event::AbortWriter, Event::AbortSession]
    );
}

#[tokio::test]
async fn finish_failure_aborts_writer_and_session() {
    let (session, events) = session(
        Faults {
            finish: true,
            ..Default::default()
        },
        1,
    );
    assert!(session.into_writer().await.unwrap().commit().await.is_err());
    assert_eq!(
        *events.lock().unwrap(),
        [Event::Finish, Event::AbortWriter, Event::AbortSession]
    );
}

#[tokio::test]
async fn publication_failure_never_aborts_potentially_published_output() {
    let (session, events) = session(
        Faults {
            publish: true,
            ..Default::default()
        },
        1,
    );
    assert!(session.into_writer().await.unwrap().commit().await.is_err());
    assert_eq!(*events.lock().unwrap(), [Event::Finish, Event::Publish]);
}

#[tokio::test]
async fn invalid_writer_counts_abort_every_writer_and_the_session() {
    for count in [0, 2] {
        let (session, events) = session(Faults::default(), count);
        assert!(session.into_writer().await.is_err());
        let mut expected = vec![Event::AbortWriter; count];
        expected.push(Event::AbortSession);
        assert_eq!(*events.lock().unwrap(), expected);
    }
}

#[tokio::test]
async fn session_abort_runs_even_if_writer_abort_fails() {
    let (session, events) = session(
        Faults {
            abort: true,
            ..Default::default()
        },
        1,
    );
    assert!(session.into_writer().await.unwrap().abort().await.is_err());
    assert_eq!(
        *events.lock().unwrap(),
        [Event::AbortWriter, Event::AbortSession]
    );
}

#[tokio::test]
async fn dropping_a_writer_never_publishes_or_runs_async_cleanup() {
    let (session, events) = session(Faults::default(), 1);
    let mut writer = session.into_writer().await.unwrap();
    writer.write(empty_batch()).await.unwrap();
    drop(writer);
    assert_eq!(*events.lock().unwrap(), [Event::Write]);
}
