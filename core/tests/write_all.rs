use std::sync::{Arc, Mutex};

use arrow_array::{ArrayRef, Int64Array};
use futures::{FutureExt, StreamExt, stream};
use open_arrow_sink_core::{
    Error, IndexCommitError, RecordBatch, Result, SchemaRef, Sink, SinkCapability, SinkExt,
    WriteAllError, WriteOptions, Writer, async_trait,
};

type Events = Arc<Mutex<Vec<String>>>;

#[derive(Clone, Copy, Default)]
struct Faults {
    open: bool,
    write: bool,
    abort: bool,
    commit: bool,
}

struct TestSink {
    events: Events,
    faults: Faults,
}

struct TestWriter {
    schema: SchemaRef,
    events: Events,
    faults: Faults,
}

fn event(events: &Events, value: impl Into<String>) {
    events.lock().unwrap().push(value.into());
}

#[async_trait]
impl Sink for TestSink {
    type Request = &'static str;

    fn capability(&self) -> SinkCapability {
        SinkCapability::default()
    }

    async fn open(
        &self,
        request: Self::Request,
        schema: SchemaRef,
        _: WriteOptions,
    ) -> Result<Box<dyn Writer>> {
        assert_eq!(request, "destination");
        event(&self.events, "open");
        if self.faults.open {
            return Err(Error::InvalidInput("open".into()));
        }
        Ok(Box::new(TestWriter {
            schema,
            events: self.events.clone(),
            faults: self.faults,
        }))
    }
}

#[async_trait]
impl Writer for TestWriter {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    async fn write(&mut self, batch: RecordBatch) -> Result<()> {
        event(&self.events, format!("write:{}", batch.num_rows()));
        if batch.schema() != self.schema {
            return Err(Error::SchemaMismatch);
        }
        if self.faults.write {
            return Err(Error::InvalidInput("write".into()));
        }
        Ok(())
    }

    async fn commit(self: Box<Self>) -> Result<()> {
        event(&self.events, "commit");
        if self.faults.commit {
            return Err(IndexCommitError::new("index").into());
        }
        Ok(())
    }

    async fn abort(self: Box<Self>) -> Result<()> {
        event(&self.events, "abort");
        if self.faults.abort {
            return Err(Error::InvalidInput("abort".into()));
        }
        Ok(())
    }
}

fn sink(faults: Faults) -> TestSink {
    TestSink {
        events: Events::default(),
        faults,
    }
}

fn batch(rows: usize) -> RecordBatch {
    RecordBatch::try_from_iter([(
        "value",
        Arc::new(Int64Array::from_iter_values(0..rows as i64)) as ArrayRef,
    )])
    .unwrap()
}

#[tokio::test]
async fn streams_sequentially_and_commits_once() {
    let sink = sink(Faults::default());
    let events = &sink.events;
    // Async streams need not be Unpin and can borrow state.
    // The next batch is not pulled ahead.
    let batches = stream::iter([2, 0, 3]).then(|rows| async move {
        event(events, format!("read:{rows}"));
        Ok::<_, std::io::Error>(batch(rows))
    });
    sink.write_all(
        "destination",
        batch(0).schema(),
        WriteOptions::default(),
        batches,
    )
    .await
    .unwrap();
    assert_eq!(
        *events.lock().unwrap(),
        [
            "open", "read:2", "write:2", "read:0", "write:0", "read:3", "write:3", "commit",
        ]
    );
}

#[tokio::test]
async fn empty_stream_still_opens_and_commits() {
    let sink = sink(Faults::default());
    sink.write_all(
        "destination",
        batch(0).schema(),
        WriteOptions::default(),
        stream::empty::<Result<RecordBatch>>(),
    )
    .await
    .unwrap();
    assert_eq!(*sink.events.lock().unwrap(), ["open", "commit"]);
}

#[derive(Debug, thiserror::Error)]
#[error("source failure")]
struct SourceError(Arc<()>);

#[tokio::test]
async fn input_error_is_preserved_even_when_abort_fails_and_input_is_not_resumed() {
    for abort in [false, true] {
        let sink = sink(Faults {
            abort,
            ..Default::default()
        });
        let token = Arc::new(());
        let batches = stream::iter([Ok(batch(2)), Err(SourceError(token.clone()))])
            .chain(stream::poll_fn(|_| panic!("input resumed after error")));
        let error = sink
            .write_all(
                "destination",
                batch(0).schema(),
                WriteOptions::default(),
                batches,
            )
            .await
            .unwrap_err();
        let WriteAllError::Input(error) = error else {
            panic!("lost input error")
        };
        assert!(Arc::ptr_eq(&error.0, &token));
        assert_eq!(*sink.events.lock().unwrap(), ["open", "write:2", "abort"]);
    }
}

#[tokio::test]
async fn write_error_aborts_without_polling_again() {
    for abort in [false, true] {
        let sink = sink(Faults {
            write: true,
            abort,
            ..Default::default()
        });
        let batches = stream::once(async { Ok::<_, std::io::Error>(batch(2)) })
            .chain(stream::poll_fn(|_| panic!("polled after failed write")));
        let error = sink
            .write_all(
                "destination",
                batch(0).schema(),
                WriteOptions::default(),
                batches,
            )
            .await
            .unwrap_err();
        assert!(
            matches!(error, WriteAllError::Sink(Error::InvalidInput(message)) if message == "write")
        );
        assert_eq!(*sink.events.lock().unwrap(), ["open", "write:2", "abort"]);
    }
}

#[tokio::test]
async fn open_failure_does_not_poll_input_or_abort() {
    let sink = sink(Faults {
        open: true,
        ..Default::default()
    });
    let batches = stream::poll_fn::<Result<RecordBatch>, _>(|_| panic!("polled before open"));
    let error = sink
        .write_all(
            "destination",
            batch(0).schema(),
            WriteOptions::default(),
            batches,
        )
        .await
        .unwrap_err();
    assert!(
        matches!(error, WriteAllError::Sink(Error::InvalidInput(message)) if message == "open")
    );
    assert_eq!(*sink.events.lock().unwrap(), ["open"]);
}

#[tokio::test]
async fn commit_failure_preserves_published_data_error_without_aborting() {
    let sink = sink(Faults {
        commit: true,
        ..Default::default()
    });
    let error = sink
        .write_all(
            "destination",
            batch(0).schema(),
            WriteOptions::default(),
            stream::iter([Ok::<_, std::io::Error>(batch(2))]),
        )
        .await
        .unwrap_err();
    assert!(
        matches!(error, WriteAllError::Sink(Error::IndexCommit(error)) if error.message == "index")
    );
    assert_eq!(*sink.events.lock().unwrap(), ["open", "write:2", "commit"]);
}

#[tokio::test]
async fn cancelling_the_future_does_not_commit_or_run_async_abort() {
    let sink = sink(Faults::default());
    let batches = stream::iter([Ok::<_, std::io::Error>(batch(2))]).chain(stream::pending());
    assert!(
        sink.write_all(
            "destination",
            batch(0).schema(),
            WriteOptions::default(),
            batches
        )
        .now_or_never()
        .is_none()
    );
    assert_eq!(*sink.events.lock().unwrap(), ["open", "write:2"]);
}
