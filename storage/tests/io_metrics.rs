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
    let path = storage.resolve(&storage.url("payload").unwrap()).unwrap();

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
