/// Outcome of one successfully published write session, across all its writers.
/// Counts describe this session, not the destination's total row count.
/// Aggregation with `+=` or [`Iterator::sum`] saturates each count at [`u64::MAX`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CommitSummary {
    /// Rows actually published, after any skipped rows have been removed.
    pub rows_committed: u64,
    /// Input rows omitted by blob download recovery.
    pub rows_skipped: u64,
    /// Input rows with at least one failed selected blob download. Includes
    /// skipped rows and rows retained by the Null policy; do not add to skipped.
    pub rows_failed_blobs: u64,
}

impl std::ops::AddAssign for CommitSummary {
    fn add_assign(&mut self, other: Self) {
        self.rows_committed = self.rows_committed.saturating_add(other.rows_committed);
        self.rows_skipped = self.rows_skipped.saturating_add(other.rows_skipped);
        self.rows_failed_blobs = self
            .rows_failed_blobs
            .saturating_add(other.rows_failed_blobs);
    }
}

impl std::iter::Sum for CommitSummary {
    fn sum<I: Iterator<Item = Self>>(iter: I) -> Self {
        iter.fold(Self::default(), |mut total, summary| {
            total += summary;
            total
        })
    }
}

impl<'a> std::iter::Sum<&'a Self> for CommitSummary {
    fn sum<I: Iterator<Item = &'a Self>>(iter: I) -> Self {
        iter.copied().sum()
    }
}

use crate::{RecordBatch, Result, SchemaRef, async_trait};

/// One write session. Drop never commits; callers explicitly commit or abort.
/// After a write error or cancellation, callers must abort or drop the writer.
#[async_trait]
pub trait Writer: Send {
    /// Return the fixed input schema established when this session was opened.
    /// Every batch passed to [`Self::write`] must
    /// match this schema, including batches with no rows.
    fn schema(&self) -> &SchemaRef;

    /// Validate the batch against the session schema before accepting it.
    /// Success means accepted, not committed. Buffering must be bounded.
    /// Native blob payloads are ordinary batch values; the backend SDK owns
    /// their physical storage layout and publication.
    async fn write(&mut self, batch: RecordBatch) -> Result<()>;

    /// Finish buffered writes and commit using the backend's documented semantics.
    /// Returns a session-local summary only after publication succeeds.
    /// Consumes the writer even on failure; no automatic cleanup or retry is provided.
    async fn commit(self: Box<Self>) -> Result<crate::CommitSummary>;

    /// Stop writing and end the session without deleting persisted output.
    async fn abort(self: Box<Self>) -> Result<()>;
}
