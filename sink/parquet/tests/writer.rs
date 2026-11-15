use std::sync::Arc;

use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use futures::TryStreamExt;
use open_arrow_sink_core::{Ordering, Sink, WriteMode, WriteOptions};
use open_arrow_sink_parquet::{ParquetDirectorySink, Url};
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

#[rstest::rstest]
#[case::default(None)]
#[case::large_target(Some(256 * 1024 * 1024))]
#[tokio::test]
async fn commit_round_trips_multiple_batches(#[case] target_file_size: Option<usize>) {
    let temp = tempfile::tempdir().unwrap();
    let directory = temp.path().join("output");
    let expected = batch();
    let mut writer = ParquetDirectorySink::default()
        .open(
            Url::from_directory_path(&directory).unwrap(),
            expected.schema(),
            WriteOptions {
                target_file_size,
                ..options()
            },
        )
        .await
        .unwrap();
    assert_eq!(writer.schema(), &expected.schema());
    writer.write(expected.slice(0, 1)).await.unwrap();
    writer.write(expected.slice(1, 2)).await.unwrap();
    assert_eq!(writer.commit().await.unwrap().rows_written, 3);

    let file = tokio::fs::File::open(directory.join("part-00000.parquet"))
        .await
        .unwrap();
    let builder = ParquetRecordBatchStreamBuilder::new(file).await.unwrap();
    // Small input batches share a row group, including nulls and strings.
    assert_eq!(builder.metadata().num_row_groups(), 1);
    let mut reader = builder.build().unwrap();
    let mut offset = 0;
    while let Some(actual) = reader.try_next().await.unwrap() {
        assert_eq!(actual, expected.slice(offset, actual.num_rows()));
        offset += actual.num_rows();
    }
    assert_eq!(offset, expected.num_rows());
}

#[tokio::test]
async fn default_row_group_limit_flushes_before_commit() {
    let rows = 1_048_576 + 8192;
    let expected = RecordBatch::try_from_iter([(
        "id",
        Arc::new(Int64Array::from_iter_values(0..rows as i64)) as ArrayRef,
    )])
    .unwrap();
    let temp = tempfile::tempdir().unwrap();
    let directory = temp.path().join("output");
    let mut writer = ParquetDirectorySink::default()
        .open(
            Url::from_directory_path(&directory).unwrap(),
            expected.schema(),
            options(),
        )
        .await
        .unwrap();
    for offset in (0..rows).step_by(8192) {
        writer.write(expected.slice(offset, 8192)).await.unwrap();
    }
    assert_eq!(writer.commit().await.unwrap().rows_written, rows as u64);
    let file = tokio::fs::File::open(directory.join("part-00000.parquet"))
        .await
        .unwrap();
    let builder = ParquetRecordBatchStreamBuilder::new(file).await.unwrap();
    assert_eq!(builder.metadata().num_row_groups(), 2);
    assert_eq!(builder.metadata().row_group(0).num_rows(), 1_048_576);
    let mut reader = builder.build().unwrap();
    let mut offset = 0;
    while let Some(actual) = reader.try_next().await.unwrap() {
        assert_eq!(actual, expected.slice(offset, actual.num_rows()));
        offset += actual.num_rows();
    }
    assert_eq!(offset, rows);
}

#[tokio::test]
async fn empty_commit_produces_readable_parquet() {
    let temp = tempfile::tempdir().unwrap();
    let directory = temp.path().join("output");
    let schema = batch().schema();
    let writer = ParquetDirectorySink::default()
        .open(
            Url::from_directory_path(&directory).unwrap(),
            schema.clone(),
            options(),
        )
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
    let mut writer = ParquetDirectorySink::default()
        .open(
            Url::from_directory_path(&directory).unwrap(),
            batch.schema(),
            WriteOptions {
                target_file_size: Some(1),
                ..options()
            },
        )
        .await
        .unwrap();
    writer.write(batch.clone()).await.unwrap();
    writer.write(batch).await.unwrap();
    writer.abort().await.unwrap();
    assert!(!tokio::fs::try_exists(&directory).await.unwrap());
    assert_eq!(tokio::fs::read_to_string(sibling).await.unwrap(), "keep");
}

#[tokio::test]
async fn target_file_size_rolls_files_without_losing_rows() {
    let expected = RecordBatch::try_from_iter([
        (
            "id",
            Arc::new(Int64Array::from_iter_values(0..9)) as ArrayRef,
        ),
        (
            "text",
            Arc::new(StringArray::from(vec!["雪"; 9])) as ArrayRef,
        ),
    ])
    .unwrap();
    let temp = tempfile::tempdir().unwrap();
    let directory = temp.path().join("output");
    let mut writer = ParquetDirectorySink::default()
        .open(
            Url::from_directory_path(&directory).unwrap(),
            expected.schema(),
            WriteOptions {
                target_file_size: Some(1),
                ..options()
            },
        )
        .await
        .unwrap();
    writer.write(expected.slice(0, 0)).await.unwrap();
    for offset in [0, 3, 6] {
        writer.write(expected.slice(offset, 3)).await.unwrap();
    }
    writer.write(expected.slice(0, 0)).await.unwrap();
    assert_eq!(writer.commit().await.unwrap().rows_written, 9);
    let mut entries = tokio::fs::read_dir(&directory).await.unwrap();
    let mut paths = Vec::new();
    while let Some(entry) = entries.next_entry().await.unwrap() {
        paths.push(entry.path());
    }
    paths.sort();
    assert_eq!(paths.len(), 3);
    let mut offset = 0;
    for path in paths {
        let file = tokio::fs::File::open(path).await.unwrap();
        let mut reader = ParquetRecordBatchStreamBuilder::new(file)
            .await
            .unwrap()
            .build()
            .unwrap();
        while let Some(actual) = reader.try_next().await.unwrap() {
            assert_eq!(actual, expected.slice(offset, actual.num_rows()));
            offset += actual.num_rows();
        }
    }
    assert_eq!(offset, 9);
}

async fn row_count(directory: &std::path::Path) -> usize {
    let mut entries = tokio::fs::read_dir(directory).await.unwrap();
    let mut rows = 0;
    while let Some(entry) = entries.next_entry().await.unwrap() {
        if entry.path().extension().is_some_and(|ext| ext == "parquet") {
            let file = tokio::fs::File::open(entry.path()).await.unwrap();
            let mut reader = ParquetRecordBatchStreamBuilder::new(file)
                .await
                .unwrap()
                .build()
                .unwrap();
            while let Some(batch) = reader.try_next().await.unwrap() {
                rows += batch.num_rows();
            }
        }
    }
    rows
}

#[rstest::rstest]
#[case::append(WriteMode::Append, 4)]
#[case::overwrite(WriteMode::Overwrite, 3)]
#[tokio::test]
async fn write_mode(#[case] mode: WriteMode, #[case] expected_rows: usize) {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("output");
    let batch = batch();
    let mut writer = ParquetDirectorySink::default()
        .open(
            Url::from_directory_path(&path).unwrap(),
            batch.schema(),
            options(),
        )
        .await
        .unwrap();
    writer.write(batch.slice(0, 1)).await.unwrap();
    writer.commit().await.unwrap();
    let mut writer = ParquetDirectorySink::default()
        .open(
            Url::from_directory_path(&path).unwrap(),
            batch.schema(),
            WriteOptions { mode, ..options() },
        )
        .await
        .unwrap();
    writer.write(batch).await.unwrap();
    writer.commit().await.unwrap();
    assert_eq!(row_count(&path).await, expected_rows);
}
