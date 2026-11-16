//! Shared observation of measured payload I/O, independent of commit results.
use std::{
    collections::BTreeMap,
    fmt,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
};

/// Measured logical payload bytes, including I/O from unsuccessful sessions.
/// Uninstrumented backends contribute no bytes; these are not network totals.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct IoSnapshot {
    pub bytes_read: u64,
    pub bytes_written: u64,
}
impl IoSnapshot {
    fn add(&mut self, other: Self) {
        self.bytes_read += other.bytes_read;
        self.bytes_written += other.bytes_written;
    }
}

/// A backend's non-resetting, thread-safe payload counter.
pub trait IoMetricsSource: Send + Sync {
    fn snapshot(&self) -> IoSnapshot;
}
#[derive(Default)]
struct State {
    completed: IoSnapshot,
    active: BTreeMap<u64, Arc<dyn IoMetricsSource>>,
}

/// Cloneable observation handle passed through `WriteOptions`.
/// Equality compares handle identity, not mutable counter values.
#[derive(Clone)]
pub struct IoMetrics(Vec<Arc<Mutex<State>>>);
impl Default for IoMetrics {
    fn default() -> Self {
        Self(vec![Arc::new(Mutex::new(State::default()))])
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
    pub fn snapshot(&self) -> IoSnapshot {
        // A combined handle records the same event to each destination.
        let state = self.0[0]
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut total = state.completed;
        for source in state.active.values() {
            total.add(source.snapshot());
        }
        total
    }
    pub fn record_read(&self, bytes: u64) {
        self.record(IoSnapshot {
            bytes_read: bytes,
            bytes_written: 0,
        });
    }
    pub fn record_write(&self, bytes: u64) {
        self.record(IoSnapshot {
            bytes_read: 0,
            bytes_written: bytes,
        });
    }
    fn record(&self, bytes: IoSnapshot) {
        for state in &self.0 {
            state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .completed
                .add(bytes);
        }
    }
    /// Observe a fresh backend counter. Keep the registration alive until all its
    /// I/O stops. Dropping it retains the final totals and releases the source.
    /// Source snapshots must not call back into this handle.
    pub fn track(&self, source: Arc<dyn IoMetricsSource>) -> IoMetricsRegistration {
        static NEXT_ID: AtomicU64 = AtomicU64::new(0);
        let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
        for state in &self.0 {
            state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .active
                .insert(id, source.clone());
        }
        IoMetricsRegistration {
            metrics: self.clone(),
            id,
        }
    }
}

/// Keeps a backend source observable and folds its final totals on drop.
#[must_use = "keep the registration alive until the source stops performing I/O"]
pub struct IoMetricsRegistration {
    metrics: IoMetrics,
    id: u64,
}
impl Drop for IoMetricsRegistration {
    fn drop(&mut self) {
        for state in &self.metrics.0 {
            let mut state = state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if let Some(source) = state.active.remove(&self.id) {
                state.completed.add(source.snapshot());
            }
        }
    }
}
