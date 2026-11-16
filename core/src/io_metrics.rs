//! Shared observation of measured payload I/O, independent of commit results.
use std::{
    fmt,
    sync::{Arc, Mutex},
};

/// Measured logical payload bytes, including I/O from unsuccessful sessions.
/// Uninstrumented backends contribute no bytes; these are not network totals.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct IoSnapshot {
    pub bytes_read: u64,
    pub bytes_written: u64,
}
/// Cloneable observation handle passed through `WriteOptions`.
/// Equality compares handle identity, not mutable counter values.
#[derive(Clone)]
pub struct IoMetrics(Vec<Arc<Mutex<IoSnapshot>>>);
impl Default for IoMetrics {
    fn default() -> Self {
        Self(vec![Arc::new(Mutex::new(IoSnapshot::default()))])
    }
}
impl fmt::Debug for IoMetrics {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("IoMetrics").field(&self.snapshot()).finish()
    }
}
impl PartialEq for IoMetrics {
    fn eq(&self, other: &Self) -> bool {
        self.0.len() == other.0.len() && self.0.iter().zip(&other.0).all(|(a, b)| Arc::ptr_eq(a, b))
    }
}
impl Eq for IoMetrics {}
impl IoMetrics {
    /// Forward observations to both handles, deduplicating shared counters.
    /// Snapshot each original handle separately; a combined handle is for recording.
    pub fn combine(mut self, other: Self) -> Self {
        for state in other.0 {
            if !self.0.iter().any(|existing| Arc::ptr_eq(existing, &state)) {
                self.0.push(state);
            }
        }
        self
    }
    /// Remove observers already accounted for at another transfer boundary.
    /// Returns None when every observer is excluded; retained observers still
    /// share their original counters.
    pub fn excluding(mut self, other: &Self) -> Option<Self> {
        self.0
            .retain(|state| !other.0.iter().any(|existing| Arc::ptr_eq(existing, state)));
        (!self.0.is_empty()).then_some(self)
    }
    pub fn snapshot(&self) -> IoSnapshot {
        // A combined handle records the same event to each destination.
        *self.0[0]
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
    pub fn record_read(&self, bytes: u64) {
        self.record(bytes, 0);
    }
    pub fn record_write(&self, bytes: u64) {
        self.record(0, bytes);
    }
    fn record(&self, read: u64, written: u64) {
        for state in &self.0 {
            let mut total = state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            total.bytes_read += read;
            total.bytes_written += written;
        }
    }
}
