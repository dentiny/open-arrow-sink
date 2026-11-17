use std::sync::{Arc, Mutex};

use open_arrow_sink_core::{
    DataWriter, Error, RecordBatch, Result, SchemaRef, SessionCommitter, WriteSession, async_trait,
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
    async fn commit(
        self: Box<Self>,
        outputs: Vec<u64>,
    ) -> Result<open_arrow_sink_core::CommitSummary> {
        self.events.lock().unwrap().push(Event::Publish);
        assert_eq!(outputs, vec![7]);
        if self.faults.publish {
            Err(Error::SchemaMismatch)
        } else {
            Ok(open_arrow_sink_core::CommitSummary {
                rows_committed: 7,
                rows_skipped: 2,
                rows_failed_blobs: 3,
            })
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
    let mut writer = session.into_writer().unwrap();
    writer.write(empty_batch()).await.unwrap();
    let summary = writer.commit().await.unwrap();
    assert_eq!(summary.rows_committed, 7);
    assert_eq!(summary.rows_skipped, 2);
    assert_eq!(summary.rows_failed_blobs, 3);

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
    let mut writer = session.into_writer().unwrap();
    assert!(writer.write(empty_batch()).await.is_err());
    assert!(writer.write(empty_batch()).await.is_err());
    assert!(writer.commit().await.is_err());
    assert_eq!(*events.lock().unwrap(), [Event::Write]);
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
    let mut writer = session.into_writer().unwrap();
    let mut pending = writer.write(empty_batch());
    let mut context = std::task::Context::from_waker(std::task::Waker::noop());
    assert!(std::future::Future::poll(pending.as_mut(), &mut context).is_pending());
    drop(pending);
    assert!(writer.write(empty_batch()).await.is_err());
    assert!(writer.commit().await.is_err());
    assert_eq!(*events.lock().unwrap(), [Event::Write]);
}

#[tokio::test]
async fn finish_failure_returns_without_aborting() {
    let (session, events) = session(
        Faults {
            finish: true,
            ..Default::default()
        },
        1,
    );
    assert!(session.into_writer().unwrap().commit().await.is_err());
    assert_eq!(*events.lock().unwrap(), [Event::Finish]);
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
    assert!(session.into_writer().unwrap().commit().await.is_err());
    assert_eq!(*events.lock().unwrap(), [Event::Finish, Event::Publish]);
}

#[tokio::test]
async fn invalid_writer_counts_return_without_aborting() {
    for count in [0, 2] {
        let (session, events) = session(Faults::default(), count);
        assert!(session.into_writer().is_err());
        assert!(events.lock().unwrap().is_empty());
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
    assert!(session.into_writer().unwrap().abort().await.is_err());
    assert_eq!(
        *events.lock().unwrap(),
        [Event::AbortWriter, Event::AbortSession]
    );
}

#[tokio::test]
async fn dropping_a_writer_never_publishes_or_runs_async_cleanup() {
    let (session, events) = session(Faults::default(), 1);
    let mut writer = session.into_writer().unwrap();
    writer.write(empty_batch()).await.unwrap();
    drop(writer);
    assert_eq!(*events.lock().unwrap(), [Event::Write]);
}
