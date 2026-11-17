//! Backend-independent write observation. Payload bytes come from shared I/O observers.
use crate::decorate::{Decorator, impl_sink};
use open_arrow_sink_core::{
    DataWriter, Error, GuardedDataWriter, Layer, RecordBatch, Result, SchemaRef, SessionCommitter,
    Sink, WriteOptions, WriteSession, Writer, async_trait,
};
use std::{
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

/// Aggregate observations across sessions opened through a metrics handle.
/// Durations sum polled operations and can overlap for parallel writers.
/// Operations rejected with `Error::Poisoned` contribute no failures or duration.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MetricsSnapshot {
    pub sessions_opened: u64,
    pub sessions_committed: u64,
    pub sessions_aborted: u64,
    pub batches_accepted: u64,
    pub rows_accepted: u64,
    pub rows_committed: u64,
    /// Failed or cancelled open/write/finish/commit/abort operations.
    /// Rejected operations returning `Error::Poisoned` are excluded.
    pub failed_operations: u64,
    /// Measured payload bytes across all observed sessions, including failures.
    /// Uninstrumented backends contribute no bytes.
    pub bytes_read: u64,
    pub bytes_written: u64,
    pub write_duration: Duration,
    pub commit_duration: Duration,
}

/// Cloneable, thread-safe handle; snapshots do not reset counters.
#[derive(Debug, Clone, Default)]
pub struct WriteMetrics {
    observations: Arc<Mutex<MetricsSnapshot>>,
    io: open_arrow_sink_core::IoMetrics,
}
impl WriteMetrics {
    pub fn snapshot(&self) -> MetricsSnapshot {
        let mut snapshot = self
            .observations
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        let io = self.io.snapshot();
        snapshot.bytes_read = io.bytes_read;
        snapshot.bytes_written = io.bytes_written;
        snapshot
    }
    fn update(&self, update: impl FnOnce(&mut MetricsSnapshot)) {
        update(
            &mut self
                .observations
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        );
    }
    fn observe_io(&self, options: &mut WriteOptions) {
        options.io_metrics = Some(match options.io_metrics.take() {
            Some(outer) => outer.combine(self.io.clone()),
            None => self.io.clone().into(),
        });
    }
    fn accepted(&self, rows: u64) {
        self.update(|s| {
            s.batches_accepted += 1;
            s.rows_accepted += rows;
        });
    }
    fn committed(&self, rows: u64) {
        self.update(|s| {
            s.sessions_committed += 1;
            s.rows_committed += rows;
        });
    }
}

#[derive(Debug, Clone, Default)]
pub struct MetricsLayer {
    metrics: WriteMetrics,
}
impl MetricsLayer {
    pub fn new(metrics: WriteMetrics) -> Self {
        Self { metrics }
    }
    pub fn metrics(&self) -> WriteMetrics {
        self.metrics.clone()
    }
}
pub struct MetricsSink<S> {
    inner: S,
    metrics: WriteMetrics,
}
impl<S: Sink> Layer<S> for MetricsLayer {
    type Layered = MetricsSink<S>;
    fn layer(&self, inner: S) -> Self::Layered {
        MetricsSink {
            inner,
            metrics: self.metrics.clone(),
        }
    }
}

#[derive(Clone, Copy)]
enum Operation {
    Write,
    Commit,
    Other,
}
pub(crate) struct Observation {
    metrics: WriteMetrics,
    operation: Operation,
    start: Instant,
    success: bool,
    ignored: bool,
}
impl Observation {
    fn new(metrics: &WriteMetrics, operation: Operation) -> Self {
        Self {
            metrics: metrics.clone(),
            operation,
            start: Instant::now(),
            success: false,
            ignored: false,
        }
    }
    fn complete<T>(mut self, result: Result<T>) -> Result<T> {
        self.success = result.is_ok();
        self.ignored = matches!(&result, Err(Error::Poisoned));
        result
    }
    fn success(mut self) {
        self.success = true;
    }
}
impl Drop for Observation {
    fn drop(&mut self) {
        if self.ignored {
            return;
        }
        let elapsed = self.start.elapsed();
        self.metrics.update(|s| {
            if !self.success {
                s.failed_operations += 1;
            }
            match self.operation {
                Operation::Write => s.write_duration += elapsed,
                Operation::Commit => s.commit_duration += elapsed,
                Operation::Other => {}
            }
        });
    }
}

impl_sink!(MetricsSink);

impl<S: Sink> Decorator for MetricsSink<S> {
    type Inner = S;
    type Context = Observation;

    fn inner(&self) -> &S {
        &self.inner
    }

    fn prepare(&self, _schema: &mut SchemaRef, options: &mut WriteOptions) -> Result<Observation> {
        self.metrics.observe_io(options);
        Ok(Observation::new(&self.metrics, Operation::Other))
    }

    fn wrap_writer(&self, observation: Observation, inner: Box<dyn Writer>) -> Box<dyn Writer> {
        self.metrics.update(|s| s.sessions_opened += 1);
        observation.success();
        Box::new(MetricsWriter {
            inner,
            metrics: self.metrics.clone(),
        })
    }

    fn wrap_session<T: Send + 'static>(
        &self,
        observation: Observation,
        session: WriteSession<T>,
    ) -> WriteSession<T> {
        self.metrics.update(|s| s.sessions_opened += 1);
        observation.success();
        WriteSession {
            writers: session
                .writers
                .into_iter()
                .map(|inner| {
                    Box::new(MetricsDataWriter {
                        inner: GuardedDataWriter::new(inner),
                        metrics: self.metrics.clone(),
                    }) as Box<dyn DataWriter<Output = T>>
                })
                .collect(),
            committer: Box::new(MetricsCommitter {
                inner: session.committer,
                metrics: self.metrics.clone(),
            }),
        }
    }
}
struct MetricsWriter {
    inner: Box<dyn Writer>,
    metrics: WriteMetrics,
}
impl MetricsWriter {
    async fn accept(&mut self, batch: RecordBatch) -> Result<()> {
        let observation = Observation::new(&self.metrics, Operation::Write);
        let rows = batch.num_rows() as u64;
        let result = self.inner.write(batch).await;
        observation.complete(result)?;
        self.metrics.accepted(rows);
        Ok(())
    }
}
#[async_trait]
impl Writer for MetricsWriter {
    fn schema(&self) -> &SchemaRef {
        self.inner.schema()
    }
    async fn write(&mut self, batch: RecordBatch) -> Result<()> {
        self.accept(batch).await
    }
    async fn commit(self: Box<Self>) -> Result<open_arrow_sink_core::CommitSummary> {
        let observation = Observation::new(&self.metrics, Operation::Commit);
        let summary = observation.complete(self.inner.commit().await)?;
        self.metrics.committed(summary.rows_committed);
        Ok(summary)
    }
    async fn abort(self: Box<Self>) -> Result<()> {
        let observation = Observation::new(&self.metrics, Operation::Other);
        observation.complete(self.inner.abort().await)?;
        self.metrics.update(|s| s.sessions_aborted += 1);
        Ok(())
    }
}

struct MetricsDataWriter<T: Send + 'static> {
    inner: GuardedDataWriter<T>,
    metrics: WriteMetrics,
}
#[async_trait]
impl<T: Send + 'static> DataWriter for MetricsDataWriter<T> {
    type Output = T;
    fn schema(&self) -> &SchemaRef {
        self.inner.schema()
    }
    async fn write(&mut self, batch: RecordBatch) -> Result<()> {
        let observation = Observation::new(&self.metrics, Operation::Write);
        let rows = batch.num_rows() as u64;
        observation.complete(self.inner.write(batch).await)?;
        self.metrics.accepted(rows);
        Ok(())
    }
    async fn finish(&mut self) -> Result<T> {
        let observation = Observation::new(&self.metrics, Operation::Other);
        observation.complete(self.inner.finish().await)
    }
    async fn abort(self: Box<Self>) -> Result<()> {
        let observation = Observation::new(&self.metrics, Operation::Other);
        observation.complete(Box::new(self.inner).abort().await)
    }
}
struct MetricsCommitter<T: Send + 'static> {
    inner: Box<dyn SessionCommitter<T>>,
    metrics: WriteMetrics,
}
#[async_trait]
impl<T: Send + 'static> SessionCommitter<T> for MetricsCommitter<T> {
    async fn commit(
        self: Box<Self>,
        outputs: Vec<T>,
    ) -> Result<open_arrow_sink_core::CommitSummary> {
        let observation = Observation::new(&self.metrics, Operation::Commit);
        let summary = observation.complete(self.inner.commit(outputs).await)?;
        self.metrics.committed(summary.rows_committed);
        Ok(summary)
    }
    async fn abort(self: Box<Self>) -> Result<()> {
        let observation = Observation::new(&self.metrics, Operation::Other);
        observation.complete(self.inner.abort().await)?;
        self.metrics.update(|s| s.sessions_aborted += 1);
        Ok(())
    }
}
