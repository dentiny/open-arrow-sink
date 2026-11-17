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

#[test]
fn collecting_observers_deduplicates_by_identity() {
    assert!(
        std::iter::empty::<IoMetrics>()
            .collect::<IoObservers>()
            .is_empty()
    );
    let first = IoMetrics::default();
    let second = IoMetrics::default();
    let observers: IoObservers = [first.clone(), second.clone(), first.clone()]
        .into_iter()
        .collect();
    assert_eq!(observers.iter().count(), 2);
    observers.record_write(5);
    assert_eq!(first.snapshot().bytes_written, 5);
    assert_eq!(second.snapshot().bytes_written, 5);
}

#[test]
fn adding_observers_retains_outer_handles_and_deduplicates_repeated_handles() {
    let outer = IoMetrics::default();
    let inner = IoMetrics::default();
    let mut options = open_arrow_sink_core::WriteOptions::default().with_io_metrics(outer.clone());
    options.add_io_metrics(inner.clone());
    options.add_io_metrics(outer.clone());
    options.io_metrics.as_ref().unwrap().record_read(9);
    assert_eq!(outer.snapshot().bytes_read, 9);
    assert_eq!(inner.snapshot().bytes_read, 9);
}
