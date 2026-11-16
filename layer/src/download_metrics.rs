//! Counters for source-download failures and skipped rows.
use std::sync::{Arc, Mutex, MutexGuard};

/// Observations across sessions using a download metrics handle.
/// A source can fail after transferring bytes. Destination failures and input
/// nulls do not count as source-download failures.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct DownloadSnapshot {
    pub blobs_failed: u64,
    /// Unique failed rows within each input batch, even with multiple failed URLs.
    pub rows_failed_blobs: u64,
    /// Rows omitted from successfully forwarded batches by SkipRow.
    pub rows_skipped: u64,
}

#[derive(Clone, Debug, Default)]
pub struct DownloadMetrics(Arc<Mutex<DownloadSnapshot>>);
impl DownloadMetrics {
    pub fn snapshot(&self) -> DownloadSnapshot {
        self.lock().clone()
    }

    pub(crate) fn failure(&self, new_row: bool) {
        let mut snapshot = self.lock();
        snapshot.blobs_failed += 1;
        snapshot.rows_failed_blobs += u64::from(new_row);
    }

    pub(crate) fn skipped(&self, rows: usize) {
        self.lock().rows_skipped += rows as u64;
    }

    fn lock(&self) -> MutexGuard<'_, DownloadSnapshot> {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}
