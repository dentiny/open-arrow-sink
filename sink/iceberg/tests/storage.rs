use open_arrow_sink_iceberg::{OpenDalStorageFactory, Storage};
use open_arrow_sink_storage::opendal;

#[tokio::test]
async fn file_io_uses_shared_operator_for_reads_ranges_and_cleanup() {
    let metrics = open_arrow_sink_core::IoMetrics::default();
    let storage = Storage::new(
        opendal::Operator::new(opendal::services::Memory::default()).unwrap(),
        "memory:///warehouse/".parse().unwrap(),
    )
    .unwrap()
    .with_io_metrics(metrics.clone());
    let file_io = OpenDalStorageFactory::new(storage.clone()).file_io();
    let url = storage.url("data/part").unwrap();
    let mut writer = file_io
        .new_output(url.as_str())
        .unwrap()
        .writer()
        .await
        .unwrap();
    writer.write("hello ".into()).await.unwrap();
    writer.write("world".into()).await.unwrap();
    assert_eq!(writer.close().await.unwrap().size, 11);
    assert_eq!(
        storage.read(&storage.resolve(&url).unwrap()).await.unwrap(),
        "hello world"
    );
    let input = file_io.new_input(url.as_str()).unwrap();
    assert_eq!(input.metadata().await.unwrap().size, 11);
    assert_eq!(
        input.reader().await.unwrap().read(6..11).await.unwrap(),
        "world"
    );
    assert_eq!(metrics.snapshot().bytes_written, 11);
    assert_eq!(metrics.snapshot().bytes_read, 16);
    let keep = storage.url("keep").unwrap();
    storage
        .write(&storage.resolve(&keep).unwrap(), "keep".into())
        .await
        .unwrap();
    file_io
        .delete_prefix(storage.url("data/").unwrap().as_str())
        .await
        .unwrap();
    assert!(
        storage
            .stat(&storage.resolve(&url).unwrap())
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        storage
            .read(&storage.resolve(&keep).unwrap())
            .await
            .unwrap(),
        "keep"
    );
    assert!(file_io.new_output("memory:///outside/file").is_err());
    assert!(file_io.serialize_all().is_err());
}
