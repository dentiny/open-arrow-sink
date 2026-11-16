use open_arrow_sink_core::{IoMetrics, IoSnapshot};

#[test]
fn cloned_observers_retain_totals_after_io_owners_drop() {
    let metrics = IoMetrics::default();
    let owner = metrics.clone();
    owner.record_read(3);
    owner.record_write(7);
    drop(owner);
    for _ in 0..2 {
        assert_eq!(
            metrics.snapshot(),
            IoSnapshot {
                bytes_read: 3,
                bytes_written: 7
            }
        );
    }
    let next = metrics.clone();
    next.record_write(5);
    drop(next);
    assert_eq!(metrics.snapshot().bytes_written, 12);
}

#[test]
fn combined_handles_forward_each_event_once() {
    let inner = IoMetrics::default();
    let outer = IoMetrics::default();
    outer.record_read(2);
    let both = inner.clone().combine(outer.clone()).combine(inner.clone());
    both.record_read(3);
    both.record_write(7);
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
    drop(both);
    assert_eq!(inner.snapshot().bytes_written, 7);
    assert_eq!(outer.snapshot().bytes_written, 7);
}
