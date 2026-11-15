use arrow_array::{ArrayRef, Int64Array, RecordBatch};
use open_arrow_sink_core::{Ordering, Sink, SinkExt, WriteOptions};
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
