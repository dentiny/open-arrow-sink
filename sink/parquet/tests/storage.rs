use arrow_array::RecordBatch;
use open_arrow_sink_core::{Sink, SinkExt, UploadConfig, WriteMode, WriteOptions};
use open_arrow_sink_layer::UnorderedWriteLayer;
use open_arrow_sink_parquet::{ParquetDirectorySink, Storage};
use std::num::NonZeroUsize;

fn storage() -> Storage {
    open_arrow_sink_test_support::memory_storage("memory:///warehouse/")
}
fn batch() -> RecordBatch {
    open_arrow_sink_test_support::int64_batch("value", vec![Some(1), Some(2), Some(3)])
}
fn options() -> WriteOptions {
    open_arrow_sink_test_support::unordered_options()
        .with_target_file_size(1)
        .with_upload(
            UploadConfig::default()
                .with_part_size(NonZeroUsize::new(16).unwrap())
                .with_concurrency(NonZeroUsize::new(2).unwrap()),
        )
}

#[rstest::rstest]
#[case::cleanup(true)]
#[case::keep(false)]
#[tokio::test]
async fn shared_storage_abort_and_keep_policy(#[case] cleanup_on_error: bool) {
    let storage = storage();
    let directory = storage.url("output/").unwrap();
    let mut writer = ParquetDirectorySink::new(storage.clone())
        .open(
            directory.clone(),
            batch().schema(),
            options().with_cleanup_on_error(cleanup_on_error),
        )
        .await
        .unwrap();
    writer.write(batch()).await.unwrap();
    writer.write(batch()).await.unwrap();
    writer.abort().await.unwrap();
    assert_eq!(
        storage
            .list(&storage.resolve(&directory).unwrap())
            .await
            .unwrap()
            .is_empty(),
        cleanup_on_error
    );
}

#[tokio::test]
async fn rolling_collision_is_preserved() {
    let storage = storage();
    let sink = ParquetDirectorySink::new(storage.clone());
    let directory = storage.url("output/").unwrap();
    let mut writer = sink
        .open(directory.clone(), batch().schema(), options())
        .await
        .unwrap();
    writer.write(batch()).await.unwrap();
    let collision = directory.join("part-0-00001.parquet").unwrap();
    storage
        .create(&storage.resolve(&collision).unwrap(), "keep".into())
        .await
        .unwrap();
    assert!(writer.write(batch()).await.is_err());
    writer.abort().await.unwrap();
    assert_eq!(
        storage
            .read(&storage.resolve(&collision).unwrap())
            .await
            .unwrap(),
        "keep"
    );
}

async fn rows(storage: &Storage, directory: &open_arrow_sink_parquet::Url) -> usize {
    open_arrow_sink_test_support::read_parquet_dir(storage, directory)
        .await
        .iter()
        .map(RecordBatch::num_rows)
        .sum()
}

#[rstest::rstest]
#[case::memory(false)]
#[case::filesystem(true)]
#[tokio::test]
async fn overwrite_is_unsupported_on_every_backend(#[case] filesystem: bool) {
    let temp = tempfile::tempdir().unwrap();
    let (sink, storage, directory) = if filesystem {
        let directory =
            open_arrow_sink_parquet::Url::from_directory_path(temp.path().join("output")).unwrap();
        let storage = Storage::from_url(&directory).unwrap();
        (ParquetDirectorySink::default(), storage, directory)
    } else {
        let storage = storage();
        let directory = storage.url("output/").unwrap();
        (
            ParquetDirectorySink::new(storage.clone()),
            storage,
            directory,
        )
    };
    let mut original = sink
        .open(directory.clone(), batch().schema(), options())
        .await
        .unwrap();
    original.write(batch()).await.unwrap();
    original.commit().await.unwrap();
    assert!(
        !sink
            .capability()
            .supported_modes
            .contains(&WriteMode::Overwrite)
    );
    let result = sink
        .layer(UnorderedWriteLayer::new(NonZeroUsize::new(3).unwrap()))
        .open(
            directory.clone(),
            batch().schema(),
            options().with_mode(WriteMode::Overwrite),
        )
        .await;
    assert!(matches!(
        result,
        Err(open_arrow_sink_core::Error::Unsupported(_))
    ));
    assert_eq!(rows(&storage, &directory).await, 3);
}

#[tokio::test]
async fn session_observer_counts_parquet_output() {
    let storage = storage();
    let metrics = open_arrow_sink_core::IoMetrics::default();
    let mut writer = ParquetDirectorySink::new(storage.clone())
        .open(
            storage.url("measured/").unwrap(),
            batch().schema(),
            options().with_io_metrics(metrics.clone()),
        )
        .await
        .unwrap();
    writer.write(batch()).await.unwrap();
    writer.commit().await.unwrap();
    assert!(metrics.snapshot().bytes_written > 0);
}
