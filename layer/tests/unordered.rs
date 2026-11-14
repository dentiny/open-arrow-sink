#![cfg(feature = "unordered")]

use arrow_array::{Int64Array, RecordBatch};
use futures::{FutureExt, channel::oneshot};
use open_arrow_sink_core::{
    Error, Ordering, ParallelSink, Result, SchemaRef, Sink, SinkCapability, SinkExt, WriteOptions,
    WriteSummary, Writer, async_trait,
};
use open_arrow_sink_layer::UnorderedWriteLayer;
use std::{
    num::NonZeroUsize,
    sync::{Arc, Mutex},
};

#[derive(Default)]
struct State {
    opened: usize,
    started: usize,
    finished: usize,
    committed: usize,
    aborted: usize,
    rows: u64,
    gates: Vec<oneshot::Sender<()>>,
    fail_write: bool,
    fail_commit: bool,
}

struct TestSink(Arc<Mutex<State>>);
struct TestWriter {
    state: Arc<Mutex<State>>,
    schema: SchemaRef,
    gate: Option<oneshot::Receiver<()>>,
    rows: u64,
}

fn error() -> Error {
    Error::Unsupported("injected failure".into())
}

#[async_trait]
impl Sink for TestSink {
    type Request = ();
    fn capability(&self) -> SinkCapability {
        SinkCapability {
            ordering: Ordering::PreserveInput,
        }
    }
    async fn open(&self, _: (), _: SchemaRef, _: WriteOptions) -> Result<Box<dyn Writer>> {
        unreachable!()
    }
}

#[async_trait]
impl ParallelSink for TestSink {
    async fn open_writers(
        &self,
        _: (),
        schema: SchemaRef,
        _: WriteOptions,
        writers: NonZeroUsize,
    ) -> Result<Vec<Box<dyn Writer>>> {
        let mut result = Vec::new();
        for _ in 0..writers.get() {
            let (sender, receiver) = oneshot::channel();
            let mut state = self.0.lock().unwrap();
            state.opened += 1;
            state.gates.push(sender);
            result.push(Box::new(TestWriter {
                state: self.0.clone(),
                schema: schema.clone(),
                gate: Some(receiver),
                rows: 0,
            }) as Box<dyn Writer>);
        }
        Ok(result)
    }
}

#[async_trait]
impl Writer for TestWriter {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }
    async fn write(&mut self, batch: RecordBatch) -> Result<()> {
        self.state.lock().unwrap().started += 1;
        if let Some(gate) = self.gate.take() {
            gate.await.unwrap();
        }
        let mut state = self.state.lock().unwrap();
        if state.fail_write {
            return Err(error());
        }
        state.finished += 1;
        self.rows += batch.num_rows() as u64;
        Ok(())
    }
    async fn commit(self: Box<Self>) -> Result<WriteSummary> {
        let mut state = self.state.lock().unwrap();
        if state.fail_commit {
            return Err(error());
        }
        state.committed += 1;
        state.rows += self.rows;
        Ok(WriteSummary {
            rows_written: self.rows,
        })
    }
    async fn abort(self: Box<Self>) -> Result<()> {
        self.state.lock().unwrap().aborted += 1;
        Ok(())
    }
}

fn batch() -> RecordBatch {
    RecordBatch::try_from_iter([("id", Arc::new(Int64Array::from(vec![1, 2, 3])) as _)]).unwrap()
}
fn options() -> WriteOptions {
    WriteOptions {
        ordering: Ordering::Unordered,
        ..Default::default()
    }
}
fn sink(state: &Arc<Mutex<State>>) -> impl Sink<Request = ()> {
    TestSink(state.clone()).layer(UnorderedWriteLayer::new(NonZeroUsize::new(2).unwrap()))
}
fn release(state: &Arc<Mutex<State>>) {
    for gate in state.lock().unwrap().gates.drain(..) {
        let _ = gate.send(());
    }
}

#[tokio::test]
async fn preopens_writers_and_bounds_concurrent_batches() {
    let state = Arc::new(Mutex::new(State::default()));
    let batch = batch();
    let mut writer = sink(&state)
        .open((), batch.schema(), options())
        .await
        .unwrap();
    assert_eq!(state.lock().unwrap().opened, 2);
    writer.write(batch.clone()).await.unwrap();
    writer.write(batch.clone()).await.unwrap();
    assert_eq!(state.lock().unwrap().started, 2);
    assert_eq!(state.lock().unwrap().finished, 0);
    {
        let third = writer.write(batch);
        futures::pin_mut!(third);
        assert!(third.as_mut().now_or_never().is_none());
        assert_eq!(state.lock().unwrap().started, 2);
        release(&state);
        third.await.unwrap();
    }
    assert_eq!(writer.commit().await.unwrap().rows_written, 9);
    let state = state.lock().unwrap();
    assert_eq!((state.committed, state.rows, state.finished), (2, 9, 3));
}

#[tokio::test]
async fn rejects_preserve_order_and_invalid_size_before_open() {
    let state = Arc::new(Mutex::new(State::default()));
    for options in [
        WriteOptions::default(),
        WriteOptions {
            target_file_size: Some(0),
            ..options()
        },
    ] {
        assert!(
            sink(&state)
                .open((), batch().schema(), options)
                .await
                .is_err()
        );
    }
    assert_eq!(state.lock().unwrap().opened, 0);
}

#[tokio::test]
async fn commit_drains_writes_and_abort_discards_them() {
    for abort in [false, true] {
        let state = Arc::new(Mutex::new(State::default()));
        let batch = batch();
        let mut writer = sink(&state)
            .open((), batch.schema(), options())
            .await
            .unwrap();
        writer.write(batch).await.unwrap();
        release(&state);
        if abort {
            writer.abort().await.unwrap();
        } else {
            assert_eq!(writer.commit().await.unwrap().rows_written, 3);
        }
        let state = state.lock().unwrap();
        assert_eq!(state.finished, 1);
        assert_eq!(
            (state.committed, state.aborted),
            if abort { (0, 2) } else { (2, 0) }
        );
    }
}

#[tokio::test]
async fn async_failure_aborts_every_writer_before_committing() {
    let state = Arc::new(Mutex::new(State {
        fail_write: true,
        ..Default::default()
    }));
    let mut writer = sink(&state)
        .open((), batch().schema(), options())
        .await
        .unwrap();
    writer.write(batch()).await.unwrap();
    writer.write(batch()).await.unwrap();
    release(&state);
    assert!(writer.commit().await.is_err());
    let state = state.lock().unwrap();
    assert_eq!((state.committed, state.aborted), (0, 2));
}

#[tokio::test]
async fn cancelled_write_makes_commit_terminal_and_cleans_up() {
    let state = Arc::new(Mutex::new(State::default()));
    let mut writer = sink(&state)
        .open((), batch().schema(), options())
        .await
        .unwrap();
    writer.write(batch()).await.unwrap();
    writer.write(batch()).await.unwrap();
    assert!(writer.write(batch()).now_or_never().is_none());
    release(&state);
    assert!(writer.write(batch()).await.is_err());
    assert!(writer.commit().await.is_err());
    let state = state.lock().unwrap();
    assert_eq!((state.committed, state.aborted), (0, 2));
}

#[tokio::test]
async fn schema_error_is_terminal_even_for_empty_batches() {
    let state = Arc::new(Mutex::new(State::default()));
    let mut writer = sink(&state)
        .open((), batch().schema(), options())
        .await
        .unwrap();
    let bad = RecordBatch::new_empty(Arc::new(arrow_schema::Schema::empty()));
    assert!(matches!(
        writer.write(bad).await,
        Err(Error::SchemaMismatch)
    ));
    assert!(writer.commit().await.is_err());
    assert_eq!(state.lock().unwrap().started, 0);
    assert_eq!(state.lock().unwrap().aborted, 2);
}

#[tokio::test]
async fn commit_failure_aborts_remaining_writers() {
    let state = Arc::new(Mutex::new(State {
        fail_commit: true,
        ..Default::default()
    }));
    let writer = sink(&state)
        .open((), batch().schema(), options())
        .await
        .unwrap();
    assert!(writer.commit().await.is_err());
    let state = state.lock().unwrap();
    assert_eq!((state.committed, state.aborted), (0, 1));
}

#[tokio::test]
async fn drop_never_commits() {
    let state = Arc::new(Mutex::new(State::default()));
    let mut writer = sink(&state)
        .open((), batch().schema(), options())
        .await
        .unwrap();
    writer.write(batch()).await.unwrap();
    drop(writer);
    release(&state);
    let state = state.lock().unwrap();
    assert_eq!((state.finished, state.committed), (0, 0));
}

#[tokio::test]
async fn available_writer_progresses_without_waiting_for_slow_writer() {
    let state = Arc::new(Mutex::new(State::default()));
    let mut writer = sink(&state)
        .open((), batch().schema(), options())
        .await
        .unwrap();
    writer.write(batch()).await.unwrap();
    writer.write(batch()).await.unwrap();
    // The first batch is on writer 1; release only writer 0.
    state.lock().unwrap().gates.remove(0).send(()).unwrap();
    assert!(writer.write(batch()).now_or_never().unwrap().is_ok());
    assert_eq!(state.lock().unwrap().started, 3);
    assert_eq!(state.lock().unwrap().finished, 2);
    release(&state);
    assert_eq!(writer.commit().await.unwrap().rows_written, 9);
}

#[tokio::test]
async fn observed_write_failure_is_terminal_and_abort_keeps_all_writers() {
    let state = Arc::new(Mutex::new(State {
        fail_write: true,
        ..Default::default()
    }));
    let mut writer = sink(&state)
        .open((), batch().schema(), options())
        .await
        .unwrap();
    writer.write(batch()).await.unwrap();
    release(&state);
    assert!(writer.write(batch()).await.is_err());
    assert!(writer.write(batch()).await.is_err());
    writer.abort().await.unwrap();
    assert_eq!(state.lock().unwrap().aborted, 2);
}
