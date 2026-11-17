use open_arrow_sink_iceberg::OpenDalStorageFactory;
use open_arrow_sink_test_support::url as fixture_url;

#[tokio::test]
async fn missing_metadata_preserves_the_shared_not_found_error_as_source() {
    let storage = open_arrow_sink_test_support::memory_warehouse();
    let url = fixture_url(&storage, "missing");
    let file_io = OpenDalStorageFactory::new(storage).file_io();
    let error = file_io
        .new_input(url.as_str())
        .unwrap()
        .metadata()
        .await
        .err()
        .expect("missing metadata must fail");
    let source = std::error::Error::source(&error).unwrap();
    assert!(matches!(
        source.downcast_ref::<open_arrow_sink_core::Error>(),
        Some(open_arrow_sink_core::Error::NotFound(_))
    ));
}

#[tokio::test]
async fn file_io_uses_shared_operator_for_reads_and_ranges() {
    let metrics = open_arrow_sink_core::IoMetrics::default();
    let storage = open_arrow_sink_test_support::memory_warehouse().with_io_metrics(metrics.clone());
    let file_io = OpenDalStorageFactory::new(storage.clone()).file_io();
    let url = fixture_url(&storage, "data/part");
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
        storage.read(&storage.path(&url).unwrap()).await.unwrap(),
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
}

#[tokio::test]
async fn deleting_prefix_preserves_siblings() {
    let storage = open_arrow_sink_test_support::memory_warehouse();
    let file_io = OpenDalStorageFactory::new(storage.clone()).file_io();
    let url = fixture_url(&storage, "data/part");
    storage
        .write(
            &open_arrow_sink_test_support::path(&storage, "data/part"),
            "data".into(),
        )
        .await
        .unwrap();
    let keep = fixture_url(&storage, "keep");
    storage
        .write(&storage.path(&keep).unwrap(), "keep".into())
        .await
        .unwrap();
    file_io
        .delete_prefix(fixture_url(&storage, "data/").as_str())
        .await
        .unwrap();
    assert!(
        storage
            .stat(&storage.path(&url).unwrap())
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        storage.read(&storage.path(&keep).unwrap()).await.unwrap(),
        "keep"
    );
}

#[test]
fn file_io_rejects_foreign_outputs() {
    let storage = open_arrow_sink_test_support::memory_warehouse();
    let file_io = OpenDalStorageFactory::new(storage).file_io();
    assert!(file_io.new_output("memory:///outside/file").is_err());
}

#[test]
fn file_io_does_not_serialize_live_operators() {
    let storage = open_arrow_sink_test_support::memory_warehouse();
    let file_io = OpenDalStorageFactory::new(storage).file_io();
    assert!(file_io.serialize_all().is_err());
}
