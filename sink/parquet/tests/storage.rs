use arrow_array::{ArrayRef, Int64Array, RecordBatch};
use open_arrow_sink_core::{Ordering, Sink, SinkExt, WriteMode, WriteOptions};
use open_arrow_sink_layer::UnorderedWriteLayer;
use open_arrow_sink_parquet::{ParquetDirectorySink, Storage};
use open_arrow_sink_storage::opendal;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::{num::NonZeroUsize, sync::Arc};

fn storage() -> Storage {
    Storage::new(
        opendal::Operator::new(opendal::services::Memory::default()).unwrap(),
        "memory:///warehouse/".parse().unwrap(),
    )
    .unwrap()
}
fn batch() -> RecordBatch {
    RecordBatch::try_from_iter([(
        "value",
        Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef,
    )])
    .unwrap()
}
fn options() -> WriteOptions {
    WriteOptions {
        ordering: Ordering::Unordered,
        target_file_size: Some(1),
        ..Default::default()
    }
}

#[tokio::test]
async fn shared_storage_parallel_rolling_round_trips() {
    let storage = storage();
    let sink = ParquetDirectorySink::new(storage.clone())
        .layer(UnorderedWriteLayer::new(NonZeroUsize::new(3).unwrap()));
    let directory = storage.url("雪 output/").unwrap();
    let mut writer = sink
        .open(directory.clone(), batch().schema(), options())
        .await
        .unwrap();
    for _ in 0..8 {
        writer.write(batch()).await.unwrap();
    }
    assert_eq!(writer.commit().await.unwrap().rows_written, 24);
    let mut rows = 0;
    for url in storage.list(&directory).await.unwrap() {
        assert!(url.path().ends_with(".parquet"));
        let bytes = storage.read(&url).await.unwrap();
        for result in ParquetRecordBatchReaderBuilder::try_new(bytes)
            .unwrap()
            .build()
            .unwrap()
        {
            rows += result.unwrap().num_rows();
        }
    }
    assert_eq!(rows, 24);
    assert!(
        sink.open(directory, batch().schema(), options())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn shared_storage_abort_and_keep_policy() {
    for cleanup_on_error in [true, false] {
        let storage = storage();
        let directory = storage.url("output/").unwrap();
        let mut writer = ParquetDirectorySink::new(storage.clone())
            .open(
                directory.clone(),
                batch().schema(),
                WriteOptions {
                    cleanup_on_error,
                    ..options()
                },
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

#[tokio::test]
async fn remote_append_validates_schema_and_abort_preserves_existing_parts() {
    let storage = storage();
    let sink = ParquetDirectorySink::new(storage.clone());
    let directory = storage.url("modes/").unwrap();
    for mode in [WriteMode::Create, WriteMode::Append] {
        let mut writer = sink
            .open(
                directory.clone(),
                batch().schema(),
                WriteOptions { mode, ..options() },
            )
            .await
            .unwrap();
        writer.write(batch()).await.unwrap();
        writer.commit().await.unwrap();
    }
    assert_eq!(rows(&storage, &directory).await, 6);
    let before = storage.list(&directory).await.unwrap();
    let wrong =
        RecordBatch::try_from_iter([("other", Arc::new(Int64Array::from(vec![1])) as ArrayRef)])
            .unwrap();
    assert!(
        sink.open(
            directory.clone(),
            wrong.schema(),
            WriteOptions {
                mode: WriteMode::Append,
                ..options()
            }
        )
        .await
        .is_err()
    );
    let mut writer = sink
        .open(
            directory.clone(),
            batch().schema(),
            WriteOptions {
                mode: WriteMode::Append,
                ..options()
            },
        )
        .await
        .unwrap();
    writer.write(batch()).await.unwrap();
    writer.abort().await.unwrap();
    assert_eq!(storage.list(&directory).await.unwrap(), before);
    assert!(
        sink.open(
            directory.clone(),
            batch().schema(),
            WriteOptions {
                mode: WriteMode::Overwrite,
                ..options()
            }
        )
        .await
        .is_err()
    );
    assert_eq!(storage.list(&directory).await.unwrap(), before);
}

#[tokio::test]
async fn filesystem_staged_overwrite_abort_preserves_visible_target() {
    let temp = tempfile::tempdir().unwrap();
    let directory =
        open_arrow_sink_parquet::Url::from_directory_path(temp.path().join("output")).unwrap();
    let storage = Storage::from_url(&directory).unwrap();
    let sink = ParquetDirectorySink::default();
    let mut original = sink
        .open(directory.clone(), batch().schema(), options())
        .await
        .unwrap();
    original.write(batch()).await.unwrap();
    original.commit().await.unwrap();
    let mut writer = sink
        .layer(UnorderedWriteLayer::new(NonZeroUsize::new(3).unwrap()))
        .open(
            directory.clone(),
            batch().schema(),
            WriteOptions {
                mode: WriteMode::Overwrite,
                ..options()
            },
        )
        .await
        .unwrap();
    writer.write(batch()).await.unwrap();
    assert_eq!(rows(&storage, &directory).await, 3);
    writer.abort().await.unwrap();
    assert_eq!(rows(&storage, &directory).await, 3);
    assert_eq!(std::fs::read_dir(temp.path()).unwrap().count(), 1);
}
