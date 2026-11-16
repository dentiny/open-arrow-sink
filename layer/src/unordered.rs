use std::num::NonZeroUsize;

use futures::{FutureExt, StreamExt, future::BoxFuture, stream::FuturesUnordered};
use open_arrow_sink_core::{
    DataWriter, Layer, Ordering, ParallelSink, RecordBatch, Result, SchemaRef, SessionCommitter,
    Sink, SinkCapability, WriteOptions, WriteSummary, Writer, async_trait, validation,
};

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
            // DataWriter has no native-blob interface to forward.
            supports_blob_streaming: false,
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
        validation::validate_ordering_options(options.ordering, Ordering::Unordered)?;
        // The backend validates other options after inner layers apply overrides.
        let session = self
            .inner
            .open_writers(request, schema.clone(), options, self.writers)
            .await?;
        if let Err(error) =
            crate::validation::validate_session(&session, self.writers.get(), &schema)
        {
            for writer in session.writers {
                let _ = writer.abort().await;
            }
            let _ = session.committer.abort().await;
            return Err(error);
        }
        Ok(Box::new(UnorderedWriter {
            schema,
            idle: session.writers,
            committer: Some(session.committer),
            pending: FuturesUnordered::new(),
            failed: false,
        }))
    }
}

type PendingWrite<T> = BoxFuture<'static, (Box<dyn DataWriter<Output = T>>, Result<()>)>;

struct UnorderedWriter<T: Send + 'static> {
    schema: SchemaRef,
    idle: Vec<Box<dyn DataWriter<Output = T>>>,
    committer: Option<Box<dyn SessionCommitter<T>>>,
    pending: FuturesUnordered<PendingWrite<T>>,
    failed: bool,
}

impl<T: Send + 'static> UnorderedWriter<T> {
    fn complete(
        &mut self,
        (writer, result): (Box<dyn DataWriter<Output = T>>, Result<()>),
    ) -> Result<()> {
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
        crate::validation::validate_writer_state(self.failed)?;
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
        let pre_commit_outcome = self
            .committer
            .as_ref()
            .expect("session has a committer")
            .pre_commit_outcome();
        if let Err(error) = crate::validation::validate_writer_state(self.failed) {
            let _ = self.cleanup().await;
            return Err(error.with_publication_outcome(pre_commit_outcome));
        }
        if let Err(error) = self.drain().await {
            let _ = self.cleanup().await;
            return Err(error.with_publication_outcome(pre_commit_outcome));
        }
        // Finish in parallel, retaining every writer until all finish results
        // are known. A preparation error must never publish a partial session.
        let mut finishing = FuturesUnordered::new();
        for mut writer in self.idle.drain(..) {
            finishing.push(async move {
                let result = writer.finish().await;
                (writer, result)
            });
        }
        let mut outputs = Vec::new();
        let mut first_error = None;
        while let Some((writer, result)) = finishing.next().await {
            self.idle.push(writer);
            match result {
                Ok(output) => outputs.push(output),
                Err(error) => {
                    first_error.get_or_insert(error);
                }
            }
        }
        if let Some(error) = first_error {
            let _ = self.cleanup().await;
            return Err(error.with_publication_outcome(pre_commit_outcome));
        }
        // From here publication and any ambiguous failure belong to the backend.
        self.committer
            .take()
            .expect("session has a committer")
            .commit(outputs)
            .await
    }

    async fn abort(mut self: Box<Self>) -> Result<()> {
        self.cleanup().await
    }
}
