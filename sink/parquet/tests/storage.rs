use arrow_array::RecordBatch;
use open_arrow_sink_core::{Ordering, Sink, SinkExt, UploadConfig, WriteMode, WriteOptions};
use open_arrow_sink_layer::UnorderedWriteLayer;
use open_arrow_sink_parquet::{ParquetDirectorySink, Storage};
use open_arrow_sink_storage::opendal;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::num::NonZeroUsize;

fn storage() -> Storage {
    Storage::new(
        opendal::Operator::new(opendal::services::Memory::default()).unwrap(),
        "memory:///warehouse/".parse().unwrap(),
    )
    .unwrap()
}
fn batch() -> RecordBatch {
    open_arrow_sink_test_support::int64_batch("value", vec![Some(1), Some(2), Some(3)])
}
fn options() -> WriteOptions {
    WriteOptions::default()
        .with_ordering(Ordering::Unordered)
        .with_target_file_size(Some(1))
        .with_upload(
            UploadConfig::default()
                .with_part_size(NonZeroUsize::new(16))
                .with_concurrency(NonZeroUsize::new(2)),
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
        storage.list(&directory).await.unwrap().is_empty(),
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
    let collision = directory.join("part-00001.parquet").unwrap();
    storage.create(&collision, "keep".into()).await.unwrap();
    assert!(writer.write(batch()).await.is_err());
    writer.abort().await.unwrap();
    assert_eq!(storage.read(&collision).await.unwrap(), "keep");
}

async fn rows(storage: &Storage, directory: &open_arrow_sink_parquet::Url) -> usize {
    let mut rows = 0;
    for file in storage
        .list(directory)
        .await
        .unwrap()
        .into_iter()
        .filter(|url| url.path().ends_with(".parquet"))
    {
        for batch in ParquetRecordBatchReaderBuilder::try_new(storage.read(&file).await.unwrap())
            .unwrap()
            .build()
            .unwrap()
        {
            rows += batch.unwrap().num_rows();
        }
    }
    rows
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
