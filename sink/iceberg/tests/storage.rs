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
    assert!(!storage.exists(&url).await.unwrap());
    assert_eq!(storage.read(&keep).await.unwrap(), "keep");
    assert!(file_io.new_output("memory:///outside/file").is_err());
    assert!(file_io.serialize_all().is_err());
}

#[tokio::test]
async fn prefix_cleanup_aborts_unfinished_writers_without_touching_other_sessions() {
    let storage = Storage::new(
        opendal::Operator::new(opendal::services::Memory::default()).unwrap(),
        "memory:///warehouse/".parse().unwrap(),
    )
    .unwrap();
    let factory = OpenDalStorageFactory::new(storage.clone());
    let file_io = factory.file_io();
    let abandoned = storage.url("session-a/abandoned").unwrap();
    let active = storage.url("session-a/active").unwrap();
    let keep = storage.url("session-ab/keep").unwrap();
    let mut abandoned_writer = file_io
        .new_output(abandoned.as_str())
        .unwrap()
        .writer()
        .await
        .unwrap();
    abandoned_writer.write("abandoned".into()).await.unwrap();
    // The native SDK drops FileWrite before calling delete_prefix on abort.
    drop(abandoned_writer);
    let mut active_writer = file_io
        .new_output(active.as_str())
        .unwrap()
        .writer()
        .await
        .unwrap();
    active_writer.write("active".into()).await.unwrap();
    let mut keep_writer = file_io
        .new_output(keep.as_str())
        .unwrap()
        .writer()
        .await
        .unwrap();
    keep_writer.write("keep".into()).await.unwrap();
    // A factory clone and a new FileIO must share the unfinished-upload registry.
    factory
        .clone()
        .file_io()
        .delete_prefix(storage.url("session-a/").unwrap().as_str())
        .await
        .unwrap();
    assert!(active_writer.write("more".into()).await.is_err());
    assert!(active_writer.close().await.is_err());
    assert!(!storage.exists(&abandoned).await.unwrap());
    assert!(!storage.exists(&active).await.unwrap());
    keep_writer.write(" more".into()).await.unwrap();
    keep_writer.close().await.unwrap();
    assert_eq!(storage.read(&keep).await.unwrap(), "keep more");
    // Repeated abort cleanup is harmless.
    file_io
        .delete_prefix(storage.url("session-a/").unwrap().as_str())
        .await
        .unwrap();
}
