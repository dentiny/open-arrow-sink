use crate::{GuardedWriter, RecordBatch, Result, SchemaRef, WriteSummary, async_trait};

/// Independent physical writer. Finishing data must not publish table/dataset
/// metadata. Errors are returned without deleting output or invoking abort.
#[async_trait]
pub trait DataWriter: Send {
    type Output: Send + 'static;
    fn schema(&self) -> &SchemaRef;
    /// Validate the fixed schema, keep buffering bounded, and make write errors
    /// or cancellation terminal. Success means accepted, not published.
    async fn write(&mut self, batch: RecordBatch) -> Result<()>;
    /// Called once, after all accepted writes complete. A failure is terminal.
    async fn finish(&mut self) -> Result<Self::Output>;
    /// Stop I/O without deleting output, including finished output.
    async fn abort(self: Box<Self>) -> Result<()>;
}

/// Backend-owned publication boundary for all writers in one session.
#[async_trait]
pub trait SessionCommitter<T: Send + 'static>: Send {
    /// Called exactly once, after every data writer finishes successfully.
    /// Errors leave output in place. Publication may have succeeded despite
    /// an error; no automatic rollback or retry is performed.
    async fn commit(self: Box<Self>, outputs: Vec<T>) -> Result<WriteSummary>;
    /// End an explicitly aborted session after all data writers have stopped.
    /// Leave persisted output in place.
    async fn abort(self: Box<Self>) -> Result<()>;
}

/// Writers and their shared publication authority, created by the backend.
/// Drop never publishes or performs asynchronous cleanup.
pub struct WriteSession<T: Send + 'static> {
    pub writers: Vec<Box<dyn DataWriter<Output = T>>>,
    pub committer: Box<dyn SessionCommitter<T>>,
}

impl<T: Send + 'static> WriteSession<T> {
    /// Adapt a one-writer session to the public writer interface.
    /// Failures return directly without invoking abort or deleting output.
    pub async fn into_writer(mut self) -> Result<Box<dyn crate::Writer>> {
        if self.writers.len() != 1 {
            return Err(crate::Error::Unsupported("expected one data writer".into()));
        }
        Ok(Box::new(GuardedWriter::new(Box::new(SessionWriter {
            writer: self.writers.pop().unwrap(),
            committer: self.committer,
        }))))
    }
}

struct SessionWriter<T: Send + 'static> {
    writer: Box<dyn DataWriter<Output = T>>,
    committer: Box<dyn SessionCommitter<T>>,
}

#[async_trait]
impl<T: Send + 'static> crate::Writer for SessionWriter<T> {
    fn schema(&self) -> &SchemaRef {
        self.writer.schema()
    }

    async fn write(&mut self, batch: RecordBatch) -> Result<()> {
        self.writer.write(batch).await?;
        Ok(())
    }

    async fn commit(mut self: Box<Self>) -> Result<WriteSummary> {
        let output = self.writer.finish().await?;
        self.committer.commit(vec![output]).await
    }

    async fn abort(self: Box<Self>) -> Result<()> {
        let writer = self.writer.abort().await;
        let session = self.committer.abort().await;
        writer.and(session)
    }
}
