use open_arrow_sink_core::{IoMetrics, IoMetricsSource, IoSnapshot};
use std::sync::Arc;

struct Source(IoMetrics);
impl IoMetricsSource for Source {
    fn snapshot(&self) -> IoSnapshot {
        self.0.snapshot()
    }
}

#[test]
fn active_sources_remain_visible_then_release_with_final_totals() {
    let metrics = IoMetrics::default();
    let counter = IoMetrics::default();
    let source = Arc::new(Source(counter.clone()));
    let weak = Arc::downgrade(&source);
    let registration = metrics.track(source);
    counter.record_read(3);
    assert_eq!(metrics.snapshot().bytes_read, 3);
    counter.record_write(7);
    assert_eq!(metrics.snapshot().bytes_written, 7);
    drop(registration);
    assert!(weak.upgrade().is_none());
    // Repeated snapshots neither reset nor duplicate the finalized counts.
    assert_eq!(
        metrics.snapshot(),
        IoSnapshot {
            bytes_read: 3,
            bytes_written: 7
        }
    );
    assert_eq!(
        metrics.snapshot(),
        IoSnapshot {
            bytes_read: 3,
            bytes_written: 7
        }
    );
    // Reusing the observer adds a new session without retaining old sources.
    let next = IoMetrics::default();
    let registration = metrics.track(Arc::new(Source(next.clone())));
    next.record_write(5);
    assert_eq!(metrics.snapshot().bytes_written, 12);
    drop(registration);
    assert_eq!(metrics.snapshot().bytes_written, 12);
}

#[test]
fn combined_handles_forward_each_event_and_source_once() {
    let inner = IoMetrics::default();
    let outer = IoMetrics::default();
    outer.record_read(2);
    let both = inner.clone().combine(outer.clone()).combine(inner.clone());
    both.record_read(3);
    let source = IoMetrics::default();
    let registration = both.track(Arc::new(Source(source.clone())));
    source.record_write(7);
    assert_eq!(
        inner.snapshot(),
        IoSnapshot {
            bytes_read: 3,
            bytes_written: 7
        }
    );
    assert_eq!(
        outer.snapshot(),
        IoSnapshot {
            bytes_read: 5,
            bytes_written: 7
        }
    );
    drop(registration);
    assert_eq!(inner.snapshot().bytes_written, 7);
    assert_eq!(outer.snapshot().bytes_written, 7);
}
