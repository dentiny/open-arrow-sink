use std::num::NonZeroUsize;

use futures::{FutureExt, StreamExt, future::BoxFuture, stream::FuturesUnordered};
use open_arrow_sink_core::{
    DataWriter, GuardedWriter, Layer, Ordering, ParallelSink, RecordBatch, Result, SchemaRef,
    SessionCommitter, Sink, SinkCapability, WriteOptions, Writer, async_trait, validation,
};

/// Pre-open independent writers and dispatch batches to available writers.
/// At most one batch per writer is in flight. Work progresses while write or
/// commit is polled; this layer does not spawn background tasks.
/// Parquet and Iceberg encoding/compression therefore runs in the caller's task:
/// multiple writers overlap I/O, but do not provide CPU parallelism. Backends
/// such as Lance may schedule their own workers. See the layer guide for
/// application-managed task scheduling and its shutdown responsibilities.
#[derive(Debug, Clone, Copy)]
pub struct UnorderedWriteLayer {
    writers: NonZeroUsize,
}

impl UnorderedWriteLayer {
    pub fn new(writers: NonZeroUsize) -> Self {
        Self { writers }
    }
}

pub struct UnorderedWriteSink<S> {
    inner: S,
    writers: NonZeroUsize,
}

impl<S: ParallelSink> Layer<S> for UnorderedWriteLayer {
    type Layered = UnorderedWriteSink<S>;

    fn layer(&self, inner: S) -> Self::Layered {
        UnorderedWriteSink {
            inner,
            writers: self.writers,
        }
    }
}

#[async_trait]
impl<S: ParallelSink> Sink for UnorderedWriteSink<S> {
    type Request = S::Request;

    fn capability(&self) -> SinkCapability {
        // DataWriter has no native-blob interface to forward.
        self.inner.capability().with_ordering(Ordering::Unordered)
    }

    async fn open(
        &self,
        request: Self::Request,
        schema: SchemaRef,
        options: WriteOptions,
    ) -> Result<Box<dyn Writer>> {
        // Reordering must be explicitly permitted, including with one writer.
        validation::validate_ordering_options(options.ordering, Ordering::Unordered)?;
        // The backend validates other options after inner layers apply overrides.
        let session = self
            .inner
            .open_writers(request, schema.clone(), options, self.writers)
            .await?
            .expect(self.writers.get(), &schema)?;
        Ok(Box::new(GuardedWriter::new(Box::new(UnorderedWriter {
            schema,
            idle: session.writers,
            committer: Some(session.committer),
            pending: FuturesUnordered::new(),
        }))))
    }
}

struct WriteCompletion<T> {
    writer: Box<dyn DataWriter<Output = T>>,
    result: Result<()>,
}

type PendingWrite<T> = BoxFuture<'static, WriteCompletion<T>>;

struct UnorderedWriter<T: Send + 'static> {
    schema: SchemaRef,
    idle: Vec<Box<dyn DataWriter<Output = T>>>,
    committer: Option<Box<dyn SessionCommitter<T>>>,
    pending: FuturesUnordered<PendingWrite<T>>,
}

impl<T: Send + 'static> UnorderedWriter<T> {
    fn complete(&mut self, WriteCompletion { writer, result }: WriteCompletion<T>) -> Result<()> {
        // Retain ownership even on error so explicit abort can stop the failed writer.
        self.idle.push(writer);
        result
    }

    fn drain_ready(&mut self) -> Result<()> {
        while let Some(Some(completed)) = self.pending.next().now_or_never() {
            self.complete(completed)?;
        }
        Ok(())
    }

    async fn drain_all(&mut self) -> Result<()> {
        let mut first_error = None;
        while let Some(completed) = self.pending.next().await {
            if let Err(error) = self.complete(completed) {
                first_error.get_or_insert(error);
            }
        }
        first_error.map_or(Ok(()), Err)
    }

    async fn stop(&mut self) -> Result<()> {
        // Finish in-flight I/O before explicitly stopping writers; never detach workers.
        let mut first_error = self.drain_all().await.err();
        for writer in self.idle.drain(..) {
            if let Err(error) = writer.abort().await {
                first_error.get_or_insert(error);
            }
        }
        if let Some(committer) = self.committer.take()
            && let Err(error) = committer.abort().await
        {
            first_error.get_or_insert(error);
        }
        first_error.map_or(Ok(()), Err)
    }
}

#[async_trait]
impl<T: Send + 'static> Writer for UnorderedWriter<T> {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    async fn write(&mut self, batch: RecordBatch) -> Result<()> {
        // The outer GuardedWriter makes cancellation and errors terminal.
        validation::validate_batch(&self.schema, &batch)?;
        self.drain_ready()?;
        if batch.num_rows() > 0 {
            if self.idle.is_empty() {
                let completed = self.pending.next().await.expect("writer set is nonempty");
                self.complete(completed)?;
            }
            let mut writer = self.idle.pop().expect("an idle writer is available");
            self.pending.push(
                async move {
                    let result = writer.write(batch).await;
                    WriteCompletion { writer, result }
                }
                .boxed(),
            );
            // Poll all pending writers, including the newly accepted batch.
            self.drain_ready()?;
        }
        Ok(())
    }

    async fn commit(mut self: Box<Self>) -> Result<open_arrow_sink_core::CommitSummary> {
        self.drain_all().await?;
        let outputs =
            futures::future::try_join_all(self.idle.iter_mut().map(|writer| writer.finish()))
                .await?;
        // From here publication and any ambiguous failure belong to the backend.
        self.committer
            .take()
            .expect("session has a committer")
            .commit(outputs)
            .await
    }

    async fn abort(mut self: Box<Self>) -> Result<()> {
        self.stop().await
    }
}
