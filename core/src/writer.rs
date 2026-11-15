use crate::{BlobPlacement, BlobWriter, RecordBatch, Result, SchemaRef, WriteSummary, async_trait};

/// One write session. Drop never commits; callers explicitly commit or abort.
/// After a write error or cancellation, callers must abort or drop the writer.
#[async_trait]
pub trait Writer: Send {
    fn schema(&self) -> &SchemaRef;

    /// Validate the batch against the session schema before accepting it.
    /// Success means accepted, not committed. Buffering must be bounded.
    async fn write(&mut self, batch: RecordBatch) -> Result<()>;

    /// Whether this writer accepts streaming blobs and their session-owned handles.
    fn supports_blob_streams(&self) -> bool {
        false
    }

    /// Start a native blob. An unfinished or failed blob poisons the write session.
    async fn create_blob(&mut self) -> Result<Box<dyn BlobWriter>> {
        Err(crate::error::unsupported_blob_streams())
    }

    /// Attach native blobs to empty, non-null cells of blob-marked binary columns.
    /// Handles from other sessions and duplicate placements must be rejected.
    async fn write_with_blobs(
        &mut self,
        batch: RecordBatch,
        blobs: Vec<BlobPlacement>,
    ) -> Result<()> {
        if blobs.is_empty() {
            return self.write(batch).await;
        }
        Err(crate::error::unsupported_blob_streams())
    }

    /// Finish buffered writes and commit using the backend's documented semantics.
    /// Consumes the writer even on failure; no automatic retry is provided.
    async fn commit(self: Box<Self>) -> Result<WriteSummary>;

    /// End the session, applying the cleanup policy from `WriteOptions`.
    /// Cleanup is best effort and cannot guarantee rollback of already visible data.
    async fn abort(self: Box<Self>) -> Result<()>;
}
