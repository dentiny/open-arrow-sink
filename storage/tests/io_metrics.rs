use open_arrow_sink_core::{IoMetrics, IoSnapshot, WriteOptions};
use open_arrow_sink_storage::{Storage, opendal};

#[tokio::test]
async fn storage_counts_read_and_write_bytes_once() {
    let metrics = IoMetrics::default();
    let storage = Storage::new(
        opendal::Operator::new(opendal::services::Memory::default()).unwrap(),
        "memory:///".parse().unwrap(),
    )
    .unwrap()
    .with_io_metrics(metrics.clone())
    .with_write_options(&WriteOptions::default().with_io_metrics(metrics.clone()));
    let path = open_arrow_sink_test_support::path(&storage, "payload");

    storage.write(&path, "abcdef".into()).await.unwrap();
    assert_eq!(storage.read_range(&path, 1..4).await.unwrap(), "bcd");
    assert_eq!(
        metrics.snapshot(),
        IoSnapshot {
            bytes_read: 3,
            bytes_written: 6
        }
    );
}

#[tokio::test]
async fn storage_fans_out_to_distinct_observers_without_changing_other_clones() {
    let outer = IoMetrics::default();
    let inner = IoMetrics::default();
    let storage = Storage::new(
        opendal::Operator::new(opendal::services::Memory::default()).unwrap(),
        "memory:///".parse().unwrap(),
    )
    .unwrap()
    .with_io_metrics(outer.clone());
    let mut options = WriteOptions::default().with_io_metrics(outer.clone());
    options.add_io_metrics(inner.clone());
    let observed = storage.clone().with_write_options(&options);
    let path = open_arrow_sink_test_support::path(&storage, "payload");
    observed.write(&path, "abcdef".into()).await.unwrap();
    assert_eq!(observed.read_range(&path, 1..4).await.unwrap(), "bcd");
    assert_eq!(
        outer.snapshot(),
        IoSnapshot {
            bytes_read: 3,
            bytes_written: 6
        }
    );
    assert_eq!(inner.snapshot(), outer.snapshot());
    storage.read(&path).await.unwrap();
    assert_eq!(outer.snapshot().bytes_read, 9);
    assert_eq!(inner.snapshot().bytes_read, 3);
}

#[tokio::test]
async fn public_single_observer_layer_constructor_remains_supported() {
    let metrics = IoMetrics::default();
    let operator = opendal::Operator::new(opendal::services::Memory::default())
        .unwrap()
        .layer(open_arrow_sink_storage::IoMetricsLayer(metrics.clone()));
    operator.write("payload", "abc").await.unwrap();
    assert_eq!(metrics.snapshot().bytes_written, 3);
}
