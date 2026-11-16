use open_arrow_sink_iceberg::{OpenDalStorageFactory, Storage};
use open_arrow_sink_storage::opendal;

#[tokio::test]
async fn file_io_uses_shared_operator_for_reads_ranges_and_cleanup() {
    let storage = Storage::new(
        opendal::Operator::new(opendal::services::Memory::default()).unwrap(),
        "memory:///warehouse/".parse().unwrap(),
    )
    .unwrap();
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
    assert_eq!(storage.read(&url).await.unwrap(), "hello world");
    let input = file_io.new_input(url.as_str()).unwrap();
    assert_eq!(input.metadata().await.unwrap().size, 11);
    assert_eq!(
        input.reader().await.unwrap().read(6..11).await.unwrap(),
        "world"
    );
    let keep = storage.url("keep").unwrap();
    storage.write(&keep, "keep".into()).await.unwrap();
    file_io
        .delete_prefix(storage.url("data/").unwrap().as_str())
        .await
        .unwrap();
    assert!(storage.stat(&url).await.unwrap().is_none());
    assert_eq!(storage.read(&keep).await.unwrap(), "keep");
    assert!(file_io.new_output("memory:///outside/file").is_err());
    assert!(file_io.serialize_all().is_err());
}
