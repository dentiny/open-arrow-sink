use crate::{RecordBatch, Result, SchemaRef, WriteSummary, async_trait};

/// Independent physical writer. Finishing data must not publish table/dataset
/// metadata. The writer remains owned by the caller until session publication,
/// so it can still be aborted after another writer fails to finish.
#[async_trait]
pub trait DataWriter: Send {
    type Output: Send + 'static;
    fn schema(&self) -> &SchemaRef;
    /// Validate the fixed schema, keep buffering bounded, and make write errors
    /// or cancellation terminal. Success means accepted, not published.
    async fn write(&mut self, batch: RecordBatch) -> Result<()>;
    /// Called once, after all accepted writes complete. A failure is terminal.
    async fn finish(&mut self) -> Result<Self::Output>;
    /// Stop I/O and apply the backend cleanup policy, including finished output.
    async fn abort(self: Box<Self>) -> Result<()>;
}

/// Backend-owned publication boundary for all writers in one session.
#[async_trait]
pub trait SessionCommitter<T: Send + 'static>: Send {
    /// Called exactly once, after every data writer finishes successfully.
    /// Owns publication-failure cleanup. Publication may have succeeded despite
    /// an error, so callers must never abort writers after entering this method.
    async fn commit(self: Box<Self>, outputs: Vec<T>) -> Result<WriteSummary>;
    /// Called after all data writers have stopped and been aborted. Must also
    /// clean up session resources when an individual writer's abort failed.
    async fn abort(self: Box<Self>) -> Result<()>;
}

/// Writers and their shared publication authority, created by the backend.
/// Drop never publishes or performs asynchronous cleanup.
pub struct WriteSession<T: Send + 'static> {
    pub writers: Vec<Box<dyn DataWriter<Output = T>>>,
    pub committer: Box<dyn SessionCommitter<T>>,
}
