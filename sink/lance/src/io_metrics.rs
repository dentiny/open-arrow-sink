use lance_io::utils::tracking_store::IOTracker;
use open_arrow_sink_core::{IoMetrics, IoMetricsRegistration, IoMetricsSource, IoSnapshot};
use std::{ops::Deref, sync::Arc};

struct Source(Arc<IOTracker>);
impl IoMetricsSource for Source {
    fn snapshot(&self) -> IoSnapshot {
        let stats = self.0.stats();
        IoSnapshot {
            bytes_read: stats.read_bytes,
            bytes_written: stats.written_bytes,
        }
    }
}

pub(crate) struct TrackedIo {
    tracker: Arc<IOTracker>,
    _registration: Option<IoMetricsRegistration>,
}
impl TrackedIo {
    pub fn new(metrics: Option<&IoMetrics>) -> Arc<Self> {
        let tracker = Arc::new(IOTracker::default());
        let registration = metrics.map(|metrics| metrics.track(Arc::new(Source(tracker.clone()))));
        Arc::new(Self {
            tracker,
            _registration: registration,
        })
    }
}
impl Deref for TrackedIo {
    type Target = IOTracker;
    fn deref(&self) -> &Self::Target {
        &self.tracker
    }
}
impl std::fmt::Debug for TrackedIo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("TrackedIo").field(&self.tracker).finish()
    }
}
