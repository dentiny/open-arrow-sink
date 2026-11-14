use std::sync::Arc;

use arrow_array::{Array, ArrayRef, Int64Array, RecordBatch};
use lance::Dataset;
use open_arrow_sink_core::{Error, Ordering, Sink, WriteOptions};
use open_arrow_sink_lance::LanceDatasetSink;

fn batch() -> RecordBatch {
    RecordBatch::try_from_iter([(
        "value",
        Arc::new(Int64Array::from(vec![Some(3), None, Some(1), Some(3)])) as ArrayRef,
    )])
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
    let path = temp.path().join("dataset");
    let expected = batch();
    let mut writer = LanceDatasetSink
        .open(path.clone(), expected.schema(), options())
        .await
        .unwrap();
    assert_eq!(writer.schema(), &expected.schema());
    writer.write(expected.slice(0, 1)).await.unwrap();
    writer.write(expected.slice(1, 3)).await.unwrap();
    assert_eq!(writer.commit().await.unwrap().rows_written, 4);
    let dataset = Dataset::open(path.to_str().unwrap()).await.unwrap();
    let actual = dataset.scan().try_into_batch().await.unwrap();
    assert_eq!(actual.schema(), expected.schema());
    let values = actual
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let mut values: Vec<_> = (0..values.len())
        .map(|i| (!values.is_null(i)).then(|| values.value(i)))
        .collect();
    values.sort_unstable();
    assert_eq!(values, vec![None, Some(1), Some(3), Some(3)]);
}

#[tokio::test]
async fn empty_commit_produces_readable_dataset() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("dataset");
    let schema = batch().schema();
    let writer = LanceDatasetSink
        .open(path.clone(), schema.clone(), options())
        .await
        .unwrap();
    assert_eq!(writer.commit().await.unwrap().rows_written, 0);
    let dataset = Dataset::open(path.to_str().unwrap()).await.unwrap();
    assert_eq!(dataset.count_rows(None).await.unwrap(), 0);
    assert_eq!(
        dataset.scan().try_into_batch().await.unwrap().schema(),
        schema
    );
}

#[tokio::test]
async fn abort_removes_dataset_and_preserves_sibling_files() {
    let temp = tempfile::tempdir().unwrap();
    let sibling = temp.path().join("keep.txt");
    std::fs::write(&sibling, "keep").unwrap();
    let path = temp.path().join("dataset");
    let batch = batch();
    let mut writer = LanceDatasetSink
        .open(path.clone(), batch.schema(), options())
        .await
        .unwrap();
    writer.write(batch).await.unwrap();
    writer.abort().await.unwrap();
    assert!(!path.exists());
    assert_eq!(std::fs::read_to_string(sibling).unwrap(), "keep");
}

#[tokio::test]
async fn schema_error_is_terminal_and_abort_cleans_up() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("dataset");
    let batch = batch();
    let mut writer = LanceDatasetSink
        .open(path.clone(), batch.schema(), options())
        .await
        .unwrap();
    let wrong =
        RecordBatch::try_from_iter([("other", Arc::new(Int64Array::from(vec![1])) as ArrayRef)])
            .unwrap();
    assert!(matches!(
        writer.write(wrong).await,
        Err(Error::SchemaMismatch)
    ));
    assert!(writer.write(batch).await.is_err());
    writer.abort().await.unwrap();
    assert!(!path.exists());
}
