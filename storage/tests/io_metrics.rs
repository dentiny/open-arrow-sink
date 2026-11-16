use futures::TryStreamExt;
use object_store::{ObjectStoreExt, path::Path};
use open_arrow_sink_core::{IoMetrics, IoSnapshot, WriteOptions};
use open_arrow_sink_storage::{Storage, Url, opendal};

#[rstest::rstest]
#[case::memory(false)]
#[case::filesystem(true)]
#[tokio::test]
async fn shared_layer_counts_storage_streams_and_native_adapters_once(#[case] filesystem: bool) {
    let directory = tempfile::tempdir().unwrap();
    let storage = if filesystem {
        Storage::new(
            opendal::Operator::new(
                opendal::services::Fs::default().root(directory.path().to_str().unwrap()),
            )
            .unwrap(),
            Url::from_directory_path(directory.path()).unwrap(),
        )
        .unwrap()
    } else {
        Storage::new(
            opendal::Operator::new(opendal::services::Memory::default()).unwrap(),
            "memory:///".parse().unwrap(),
        )
        .unwrap()
    };
    let original = storage.clone();
    let metrics = IoMetrics::default();
    let outer = IoMetrics::default();
    // The same observer supplied through storage and options must not double count.
    let storage = storage.with_io_metrics(metrics.clone()).with_write_options(
        &WriteOptions::default().with_io_metrics(metrics.clone().combine(outer.clone())),
    );
    let path = storage.resolve(&storage.url("payload").unwrap()).unwrap();
    storage.write(&path, "abcdef".into()).await.unwrap();
    assert_eq!(metrics.snapshot().bytes_written, 6);
    assert_eq!(storage.read_range(&path, 1..4).await.unwrap(), "bcd");
    let mut reader = storage
        .operator()
        .reader(path.as_str())
        .await
        .unwrap()
        .into_stream(..)
        .await
        .unwrap();
    let mut size = 0;
    while let Some(chunk) = reader.try_next().await.unwrap() {
        size += chunk.len();
    }
    assert_eq!(size, 6);
    storage
        .object_store()
        .put(&Path::from("native"), "xyz".into())
        .await
        .unwrap();
    assert_eq!(
        metrics.snapshot(),
        IoSnapshot {
            bytes_read: 9,
            bytes_written: 9
        }
    );
    assert_eq!(outer.snapshot(), metrics.snapshot());
    storage.stat(&path).await.unwrap();
    let missing = storage.resolve(&storage.url("missing").unwrap()).unwrap();
    assert!(storage.read(&missing).await.is_err());
    // Reads through the original clone are outside this observer's scope.
    original.read(&path).await.unwrap();
    storage.delete(&path).await.unwrap();
    drop(reader);
    drop(storage);
    assert_eq!(
        metrics.snapshot(),
        IoSnapshot {
            bytes_read: 9,
            bytes_written: 9
        }
    );
}
