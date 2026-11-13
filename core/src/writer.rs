use crate::{RecordBatch, Result, SchemaRef, WriteSummary, async_trait};

/// One write session. Drop never commits; callers explicitly commit or abort.
/// After a write error or cancellation, callers must abort or drop the writer.
#[async_trait]
pub trait Writer: Send {
    fn schema(&self) -> &SchemaRef;

    /// Validate the batch against the session schema before accepting it.
    /// Success means accepted, not committed. Buffering must be bounded.
    async fn write(&mut self, batch: RecordBatch) -> Result<()>;

    /// Finish buffered writes and commit using the backend's documented semantics.
    /// Consumes the writer even on failure; no automatic retry is provided.
    async fn commit(self: Box<Self>) -> Result<WriteSummary>;

    /// Best-effort cleanup. This cannot guarantee rollback of already visible data.
    async fn abort(self: Box<Self>) -> Result<()>;
}
