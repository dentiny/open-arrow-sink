use std::{
    future::Future,
    pin::pin,
    sync::{Arc, Mutex},
    task::{Context, Poll, Waker},
};

use open_arrow_sink_core::{
    BlobPlacement, BlobWriter, DataWriter, Error, GuardedDataWriter, GuardedWriter, RecordBatch,
    Result, SchemaRef, WriteSummary, Writer, async_trait,
};

type Events = Arc<Mutex<Vec<&'static str>>>;

#[derive(Clone, Copy)]
enum Behavior {
    Success,
    Error,
    Pending,
}
struct Probe {
    schema: SchemaRef,
    behavior: Behavior,
    events: Events,
}
impl Probe {
    async fn operation(&self, event: &'static str) -> Result<()> {
        self.events.lock().unwrap().push(event);
        match self.behavior {
            Behavior::Success => Ok(()),
            Behavior::Error => Err(Error::SchemaMismatch),
            Behavior::Pending => std::future::pending().await,
        }
    }
}
#[async_trait]
impl Writer for Probe {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }
    async fn write(&mut self, _: RecordBatch) -> Result<()> {
        self.operation("write").await
    }
    async fn write_with_blobs(&mut self, _: RecordBatch, _: Vec<BlobPlacement>) -> Result<()> {
        self.operation("placements").await
    }
    async fn create_blob(&mut self) -> Result<Box<dyn BlobWriter>> {
        self.operation("blob").await?;
        Err(Error::Unsupported("probe has no blob output".into()))
    }
    async fn commit(self: Box<Self>) -> Result<WriteSummary> {
        self.operation("publish").await?;
        Ok(WriteSummary::default())
    }
    async fn abort(self: Box<Self>) -> Result<()> {
        self.events.lock().unwrap().push("abort");
        Ok(())
    }
}
#[async_trait]
impl DataWriter for Probe {
    type Output = ();
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }
    async fn write(&mut self, _: RecordBatch) -> Result<()> {
        self.operation("write").await
    }
    async fn finish(&mut self) -> Result<()> {
        self.operation("finish").await
    }
    async fn abort(self: Box<Self>) -> Result<()> {
        Writer::abort(self).await
    }
}
fn probe(behavior: Behavior) -> (Box<Probe>, Events) {
    let events = Arc::new(Mutex::new(Vec::new()));
    (
        Box::new(Probe {
            schema: Arc::new(Default::default()),
            behavior,
            events: events.clone(),
        }),
        events,
    )
}
fn batch() -> RecordBatch {
    RecordBatch::new_empty(Arc::new(Default::default()))
}
fn cancel(future: impl Future) {
    let mut future = pin!(future);
    assert!(matches!(
        future
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop())),
        Poll::Pending
    ));
}

#[tokio::test]
async fn writer_errors_and_cancellation_abort_instead_of_publishing() {
    for behavior in [Behavior::Error, Behavior::Pending] {
        // Every entry point is guarded, including errors before any backend I/O.
        for operation in 0..3 {
            let (inner, events) = probe(behavior);
            let mut writer = GuardedWriter::new(inner);
            let future = async {
                match operation {
                    0 => writer.write(batch()).await,
                    1 => writer.write_with_blobs(batch(), vec![]).await,
                    _ => writer.create_blob().await.map(|_| ()),
                }
            };
            match behavior {
                Behavior::Pending => cancel(future),
                _ => assert!(matches!(future.await, Err(Error::SchemaMismatch))),
            }
            assert!(matches!(writer.write(batch()).await, Err(Error::Poisoned)));
            assert!(matches!(writer.create_blob().await, Err(Error::Poisoned)));
            assert!(matches!(
                writer.write_with_blobs(batch(), vec![]).await,
                Err(Error::Poisoned)
            ));
            assert!(matches!(
                Box::new(writer).commit().await,
                Err(Error::Poisoned)
            ));
            let events = events.lock().unwrap();
            assert_eq!(events.len(), 2);
            assert_eq!(events[1], "abort");
        }
    }
}

#[tokio::test]
async fn successful_writes_restore_usability_and_allow_publication() {
    let (inner, events) = probe(Behavior::Success);
    let mut writer = GuardedWriter::new(inner);
    writer.write(batch()).await.unwrap();
    writer.write_with_blobs(batch(), vec![]).await.unwrap();
    Box::new(writer).commit().await.unwrap();
    assert_eq!(*events.lock().unwrap(), ["write", "placements", "publish"]);
}

#[tokio::test]
async fn data_finish_is_terminal_even_on_success_and_abort_remains_available() {
    for behavior in [Behavior::Success, Behavior::Error, Behavior::Pending] {
        let (inner, events) = probe(behavior);
        let mut writer = GuardedDataWriter::new(inner);
        match behavior {
            Behavior::Success => writer.finish().await.unwrap(),
            Behavior::Error => assert!(matches!(writer.finish().await, Err(Error::SchemaMismatch))),
            Behavior::Pending => cancel(writer.finish()),
        }
        assert!(matches!(writer.finish().await, Err(Error::Poisoned)));
        assert!(matches!(writer.write(batch()).await, Err(Error::Poisoned)));
        Box::new(writer).abort().await.unwrap();
        assert_eq!(*events.lock().unwrap(), ["finish", "abort"]);
    }
}

#[tokio::test]
async fn cancelled_data_write_does_not_poison_an_independent_writer() {
    let (inner, events) = probe(Behavior::Pending);
    let mut cancelled = GuardedDataWriter::new(inner);
    let (inner, _) = probe(Behavior::Success);
    let mut independent = GuardedDataWriter::new(inner);
    cancel(cancelled.write(batch()));
    assert!(matches!(cancelled.finish().await, Err(Error::Poisoned)));
    independent.write(batch()).await.unwrap();
    independent.finish().await.unwrap();
    Box::new(cancelled).abort().await.unwrap();
    assert_eq!(*events.lock().unwrap(), ["write", "abort"]);
}

#[tokio::test]
async fn publication_failure_does_not_abort_ambiguous_output() {
    let (inner, events) = probe(Behavior::Error);
    let writer = GuardedWriter::new(inner);
    assert!(matches!(
        Box::new(writer).commit().await,
        Err(Error::SchemaMismatch)
    ));
    assert_eq!(*events.lock().unwrap(), ["publish"]);
}
