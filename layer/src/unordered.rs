use std::num::NonZeroUsize;

use futures::{FutureExt, StreamExt, future::BoxFuture, stream::FuturesUnordered};
use open_arrow_sink_core::{
    Layer, Ordering, ParallelSink, RecordBatch, Result, SchemaRef, Sink, SinkCapability,
    WriteOptions, WriteSummary, Writer, async_trait, validation,
};

use crate::error::failed;

/// Pre-open independent writers and dispatch batches to available writers.
/// At most one batch per writer is in flight. Work progresses while write or
/// commit is polled; this layer does not spawn background tasks.
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
        SinkCapability {
            ordering: Ordering::Unordered,
            ..self.inner.capability()
        }
    }

    async fn open(
        &self,
        request: Self::Request,
        schema: SchemaRef,
        options: WriteOptions,
    ) -> Result<Box<dyn Writer>> {
        // Reordering must be explicitly permitted, including with one writer.
        validation::validate_options(&options, self.capability())?;
        let writers = self
            .inner
            .open_writers(request, schema.clone(), options, self.writers)
            .await?;
        Ok(Box::new(UnorderedWriter {
            schema,
            idle: writers,
            pending: FuturesUnordered::new(),
            failed: false,
        }))
    }
}

type PendingWrite = BoxFuture<'static, (Box<dyn Writer>, Result<()>)>;

struct UnorderedWriter {
    schema: SchemaRef,
    idle: Vec<Box<dyn Writer>>,
    pending: FuturesUnordered<PendingWrite>,
    failed: bool,
}

impl UnorderedWriter {
    fn complete(&mut self, (writer, result): (Box<dyn Writer>, Result<()>)) -> Result<()> {
        // Retain ownership even on error so abort can clean up the failed writer.
        self.idle.push(writer);
        result
    }

    async fn drain(&mut self) -> Result<()> {
        let mut first_error = None;
        while let Some(completed) = self.pending.next().await {
            if let Err(error) = self.complete(completed) {
                first_error.get_or_insert(error);
            }
        }
        first_error.map_or(Ok(()), Err)
    }

    async fn cleanup(&mut self) -> Result<()> {
        // Finish in-flight I/O before cleanup; never detach workers.
        let _ = self.drain().await;
        let mut first_error = None;
        for writer in self.idle.drain(..) {
            if let Err(error) = writer.abort().await {
                first_error.get_or_insert(error);
            }
        }
        first_error.map_or(Ok(()), Err)
    }
}

#[async_trait]
impl Writer for UnorderedWriter {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    async fn write(&mut self, batch: RecordBatch) -> Result<()> {
        if self.failed {
            return Err(failed());
        }
        // Cancellation and every error make the session terminal.
        self.failed = true;
        validation::validate_batch(&self.schema, &batch)?;
        while let Some(Some(completed)) = self.pending.next().now_or_never() {
            self.complete(completed)?;
        }
        if batch.num_rows() > 0 {
            if self.idle.is_empty() {
                let completed = self.pending.next().await.expect("writer set is nonempty");
                self.complete(completed)?;
            }
            let mut writer = self.idle.pop().expect("an idle writer is available");
            self.pending.push(
                async move {
                    let result = writer.write(batch).await;
                    (writer, result)
                }
                .boxed(),
            );
            // Poll all pending writers, including the newly accepted batch.
            while let Some(Some(completed)) = self.pending.next().now_or_never() {
                self.complete(completed)?;
            }
        }
        self.failed = false;
        Ok(())
    }

    async fn commit(mut self: Box<Self>) -> Result<WriteSummary> {
        if self.failed {
            let _ = self.cleanup().await;
            return Err(failed());
        }
        if let Err(error) = self.drain().await {
            let _ = self.cleanup().await;
            return Err(error);
        }
        let mut summary = WriteSummary::default();
        while let Some(writer) = self.idle.pop() {
            match writer.commit().await {
                Ok(result) => summary.rows_written += result.rows_written,
                Err(error) => {
                    let _ = self.cleanup().await;
                    return Err(error);
                }
            }
        }
        Ok(summary)
    }

    async fn abort(mut self: Box<Self>) -> Result<()> {
        self.cleanup().await
    }
}
