use std::sync::Arc;

use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use futures::TryStreamExt;
use open_arrow_sink_core::{Ordering, Sink, WriteOptions};
use open_arrow_sink_parquet::ParquetDirectorySink;
use parquet::arrow::ParquetRecordBatchStreamBuilder;

fn batch() -> RecordBatch {
    RecordBatch::try_from_iter([
        (
            "id",
            Arc::new(Int64Array::from(vec![Some(3), None, Some(1)])) as ArrayRef,
        ),
        (
            "text",
            Arc::new(StringArray::from(vec![Some("雪"), Some(""), None])) as ArrayRef,
        ),
    ])
    .unwrap()
}

fn options() -> WriteOptions {
    WriteOptions {
        ordering: Ordering::Unordered,
        ..WriteOptions::default()
    }
}

#[tokio::test]
async fn commit_round_trips_multiple_batches() {
    let temp = tempfile::tempdir().unwrap();
    let directory = temp.path().join("output");
    let expected = batch();
    let mut writer = ParquetDirectorySink
        .open(directory.clone(), expected.schema(), options())
        .await
        .unwrap();
    assert_eq!(writer.schema(), &expected.schema());
    writer.write(expected.slice(0, 1)).await.unwrap();
    writer.write(expected.slice(1, 2)).await.unwrap();
    assert_eq!(writer.commit().await.unwrap().rows_written, 3);

    let file = tokio::fs::File::open(directory.join("part-00000.parquet"))
        .await
        .unwrap();
    let mut reader = ParquetRecordBatchStreamBuilder::new(file)
        .await
        .unwrap()
        .build()
        .unwrap();
    let mut offset = 0;
    while let Some(actual) = reader.try_next().await.unwrap() {
        assert_eq!(actual, expected.slice(offset, actual.num_rows()));
        offset += actual.num_rows();
    }
    assert_eq!(offset, expected.num_rows());
}

#[tokio::test]
async fn empty_commit_produces_readable_parquet() {
    let temp = tempfile::tempdir().unwrap();
    let directory = temp.path().join("output");
    let schema = batch().schema();
    let writer = ParquetDirectorySink
        .open(directory.clone(), schema.clone(), options())
        .await
        .unwrap();
    assert_eq!(writer.commit().await.unwrap().rows_written, 0);
    let file = tokio::fs::File::open(directory.join("part-00000.parquet"))
        .await
        .unwrap();
    let builder = ParquetRecordBatchStreamBuilder::new(file).await.unwrap();
    assert_eq!(builder.schema(), &schema);
    assert!(builder.build().unwrap().try_next().await.unwrap().is_none());
}

#[tokio::test]
async fn abort_removes_output_and_preserves_sibling_files() {
    let temp = tempfile::tempdir().unwrap();
    let sibling = temp.path().join("keep.txt");
    tokio::fs::write(&sibling, "keep").await.unwrap();
    let directory = temp.path().join("output");
    let batch = batch();
    let mut writer = ParquetDirectorySink
        .open(directory.clone(), batch.schema(), options())
        .await
        .unwrap();
    writer.write(batch).await.unwrap();
    writer.abort().await.unwrap();
    assert!(!tokio::fs::try_exists(&directory).await.unwrap());
    assert_eq!(tokio::fs::read_to_string(sibling).await.unwrap(), "keep");
}
