use open_arrow_sink_core::{IoMetrics, IoObservers, IoSnapshot};

#[test]
fn clones_share_counters() {
    let metrics = IoMetrics::default();
    metrics.clone().record_read(3);
    metrics.clone().record_write(7);
    assert_eq!(
        metrics.snapshot(),
        IoSnapshot {
            bytes_read: 3,
            bytes_written: 7
        }
    );
}

#[test]
fn combining_handles_deduplicates_shared_counters() {
    let inner = IoMetrics::default();
    let outer = IoMetrics::default();
    let both = IoObservers::from(inner.clone())
        .combine(outer.clone())
        .combine(inner.clone());
    both.record_read(3);
    both.record_write(7);
    assert_eq!(
        inner.snapshot(),
        IoSnapshot {
            bytes_read: 3,
            bytes_written: 7
        }
    );
    assert_eq!(outer.snapshot(), inner.snapshot());
}
