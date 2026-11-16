//! Event-recording parallel sink for testing layers and session adapters.
//! Configuration controls failures and body gates; observations retain dispatch
//! identities separately from directly comparable lifecycle events.

use futures::channel::oneshot;
use open_arrow_sink_core::{
    DataWriter, Error, Ordering, ParallelSink, RecordBatch, Result, SchemaRef, SessionCommitter,
    Sink, SinkCapability, WriteOptions, WriteSession, Writer, async_trait,
};
use std::{
    collections::HashSet,
    num::NonZeroUsize,
    sync::{Arc, Mutex},
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Event {
    Open,
    WriteStarted,
    WriteCompleted,
    Finish,
    Abort,
    RequestedOrdering(Ordering),
    CommitAttempt,
    Commit { rows: u64 },
    SessionAbort,
}
/// Failure injection is separate from observations; multiple failures can coexist.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Failure {
    Write,
    FirstFinish,
    Abort,
    Commit,
}

#[derive(Clone, Copy, Default)]
pub enum SessionDefect {
    #[default]
    None,
    WrongCount,
    WrongSchema,
}

pub struct Config {
    pub failures: HashSet<Failure>,
    pub defect: SessionDefect,
    pub ordering: Ordering,
    pub supports_blobs: bool,
    pub gated: bool,
    pub record_io: bool,
    pub write_bytes: u64,
    pub commit_read_bytes: u64,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            failures: HashSet::new(),
            defect: SessionDefect::None,
            ordering: Ordering::PreserveInput,
            supports_blobs: false,
            gated: true,
            record_io: true,
            write_bytes: 0,
            commit_read_bytes: 0,
        }
    }
}
#[derive(Default)]
pub struct State {
    pub config: Config,
    pub events: Vec<Event>,
    pub started_writers: Vec<usize>,
    pub gates: Vec<oneshot::Sender<()>>,
    pub schemas: Vec<SchemaRef>,
}
impl State {
    pub fn shared(config: Config) -> SharedState {
        Arc::new(Mutex::new(Self {
            config,
            ..Default::default()
        }))
    }
    /// Release the first dispatched write, independent of scheduler writer order.
    pub fn release_first(&mut self) -> usize {
        let id = self.started_writers[0];
        self.gates.remove(id).send(()).unwrap();
        id
    }
    pub fn failing(operations: impl IntoIterator<Item = Failure>) -> SharedState {
        Self::shared(Config {
            failures: operations.into_iter().collect(),
            ..Default::default()
        })
    }
    pub fn count(&self, expected: Event) -> usize {
        self.events
            .iter()
            .filter(|event| **event == expected)
            .count()
    }
    pub fn requested_ordering(&self) -> Option<Ordering> {
        self.events.iter().find_map(|event| {
            if let Event::RequestedOrdering(value) = event {
                Some(*value)
            } else {
                None
            }
        })
    }
    pub fn committed_rows(&self) -> Option<u64> {
        self.events.iter().find_map(|event| {
            if let Event::Commit { rows } = event {
                Some(*rows)
            } else {
                None
            }
        })
    }
}
pub type SharedState = Arc<Mutex<State>>;
pub struct TestSink(pub SharedState);
struct TestWriter {
    id: usize,
    state: SharedState,
    schema: SchemaRef,
    gate: Option<oneshot::Receiver<()>>,
    rows: u64,
    io: Option<open_arrow_sink_core::IoObservers>,
}
fn failure() -> Error {
    Error::Unsupported("injected failure".into())
}

#[async_trait]
impl Sink for TestSink {
    type Request = ();
    fn capability(&self) -> SinkCapability {
        let state = self.0.lock().unwrap();
        SinkCapability::default()
            .with_supported_modes(&[open_arrow_sink_core::WriteMode::Create])
            .with_ordering(state.config.ordering)
            .with_supports_blob_handle(state.config.supports_blobs)
    }
    async fn open(
        &self,
        request: (),
        schema: SchemaRef,
        options: WriteOptions,
    ) -> Result<Box<dyn Writer>> {
        self.open_writer(request, schema, options).await
    }
}
#[async_trait]
impl ParallelSink for TestSink {
    type Output = u64;
    async fn open_writers(
        &self,
        _: (),
        schema: SchemaRef,
        options: WriteOptions,
        writers: NonZeroUsize,
    ) -> Result<WriteSession<u64>> {
        open_arrow_sink_core::validation::validate_options(&schema, &options, self.capability())?;
        let mut state = self.0.lock().unwrap();
        state
            .events
            .push(Event::RequestedOrdering(options.ordering));
        state.schemas.push(schema.clone());
        let io = options.io_metrics.filter(|_| state.config.record_io);
        let count = if matches!(state.config.defect, SessionDefect::WrongCount) {
            0
        } else {
            writers.get()
        };
        let mut result = Vec::new();
        for id in 0..count {
            let (sender, receiver) = oneshot::channel();
            state.events.push(Event::Open);
            let gate = if state.config.gated {
                state.gates.push(sender);
                Some(receiver)
            } else {
                None
            };
            result.push(Box::new(TestWriter {
                id,
                state: self.0.clone(),
                schema: if matches!(state.config.defect, SessionDefect::WrongSchema) {
                    Arc::new(arrow_schema::Schema::empty())
                } else {
                    schema.clone()
                },
                gate,
                rows: 0,
                io: io.clone(),
            }) as Box<dyn DataWriter<Output = u64>>);
        }
        Ok(WriteSession {
            writers: result,
            committer: Box::new(TestCommitter(self.0.clone(), io)),
        })
    }
}
#[async_trait]
impl DataWriter for TestWriter {
    type Output = u64;
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }
    async fn write(&mut self, batch: RecordBatch) -> Result<()> {
        open_arrow_sink_core::validation::validate_batch(&self.schema, &batch)?;
        if let Some(io) = &self.io {
            io.record_write(self.state.lock().unwrap().config.write_bytes);
        }
        {
            let mut state = self.state.lock().unwrap();
            state.events.push(Event::WriteStarted);
            state.started_writers.push(self.id);
        }
        if let Some(gate) = self.gate.take() {
            gate.await.unwrap();
        }
        let mut state = self.state.lock().unwrap();
        if state.config.failures.contains(&Failure::Write) {
            return Err(Error::Unsupported(format!(
                "injected write failure for writer {}",
                self.id
            )));
        }
        state.events.push(Event::WriteCompleted);
        self.rows += batch.num_rows() as u64;
        Ok(())
    }
    async fn finish(&mut self) -> Result<u64> {
        let mut state = self.state.lock().unwrap();
        state.events.push(Event::Finish);
        if state.config.failures.contains(&Failure::FirstFinish) && state.count(Event::Finish) == 1
        {
            return Err(failure());
        }
        Ok(self.rows)
    }
    async fn abort(self: Box<Self>) -> Result<()> {
        let mut state = self.state.lock().unwrap();
        state.events.push(Event::Abort);
        if state.config.failures.contains(&Failure::Abort) {
            return Err(failure());
        }
        Ok(())
    }
}
struct TestCommitter(SharedState, Option<open_arrow_sink_core::IoObservers>);
#[async_trait]
impl SessionCommitter<u64> for TestCommitter {
    async fn commit(self: Box<Self>, outputs: Vec<u64>) -> Result<()> {
        let mut state = self.0.lock().unwrap();
        assert_eq!(state.count(Event::Finish), state.count(Event::Open));
        state.events.push(Event::CommitAttempt);
        if let Some(io) = &self.1 {
            io.record_read(state.config.commit_read_bytes);
        }
        if state.config.failures.contains(&Failure::Commit) {
            return Err(failure());
        }
        let rows = outputs.into_iter().sum();
        state.events.push(Event::Commit { rows });
        Ok(())
    }
    async fn abort(self: Box<Self>) -> Result<()> {
        self.0.lock().unwrap().events.push(Event::SessionAbort);
        Ok(())
    }
}
pub fn release(state: &SharedState) {
    for gate in state.lock().unwrap().gates.drain(..) {
        let _ = gate.send(());
    }
}
