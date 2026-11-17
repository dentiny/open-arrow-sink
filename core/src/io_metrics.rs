//! Shared observation of measured payload I/O, independent of commit results.
use std::{
    fmt,
    sync::{
        Arc,
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
/// Cloneable observation handle. Equality compares counter identity.
#[derive(Clone, Default)]
pub struct IoMetrics(Arc<Counters>);

#[derive(Default)]
struct Counters {
    read: AtomicU64,
    written: AtomicU64,
}
impl fmt::Debug for IoMetrics {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("IoMetrics").field(&self.snapshot()).finish()
    }
}
impl PartialEq for IoMetrics {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}
impl Eq for IoMetrics {}
impl IoMetrics {
    /// Each counter is read independently; concurrent I/O can advance between loads.
    pub fn snapshot(&self) -> IoSnapshot {
        IoSnapshot {
            bytes_read: self.0.read.load(Ordering::Relaxed),
            bytes_written: self.0.written.load(Ordering::Relaxed),
        }
    }
    pub fn record_read(&self, bytes: u64) {
        self.0.read.fetch_add(bytes, Ordering::Relaxed);
    }
    pub fn record_write(&self, bytes: u64) {
        self.0.written.fetch_add(bytes, Ordering::Relaxed);
    }
}

/// Distinct observers forwarded through nested layers and download boundaries.
/// A collection has no snapshot: every handle owns exactly one pair of counters.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct IoObservers(Vec<IoMetrics>);
impl From<IoMetrics> for IoObservers {
    fn from(metrics: IoMetrics) -> Self {
        Self(vec![metrics])
    }
}
impl FromIterator<IoMetrics> for IoObservers {
    fn from_iter<T: IntoIterator<Item = IoMetrics>>(iter: T) -> Self {
        iter.into_iter().fold(Self::default(), Self::combine)
    }
}
impl IoObservers {
    /// Whether this collection contains no observation handles.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
    /// Add distinct handles, preserving their identity and counters.
    pub fn combine(mut self, other: impl Into<Self>) -> Self {
        for metrics in other.into().0 {
            if !self.0.contains(&metrics) {
                self.0.push(metrics);
            }
        }
        self
    }
    /// Remove handles already counted at another transfer boundary.
    pub fn excluding(mut self, other: &Self) -> Option<Self> {
        self.0.retain(|metrics| !other.0.contains(metrics));
        (!self.0.is_empty()).then_some(self)
    }
    pub fn iter(&self) -> impl Iterator<Item = &IoMetrics> {
        self.0.iter()
    }
    pub fn record_read(&self, bytes: u64) {
        for metrics in &self.0 {
            metrics.record_read(bytes);
        }
    }
    pub fn record_write(&self, bytes: u64) {
        for metrics in &self.0 {
            metrics.record_write(bytes);
        }
    }
}
