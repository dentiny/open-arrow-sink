use std::{num::NonZeroUsize, path::Path, sync::Arc};

use arrow_array::{Array, ArrayRef, Int64Array, RecordBatch, StringArray};
use futures::TryStreamExt;
use open_arrow_sink_core::{Ordering, Sink, SinkExt, WriteOptions};
use open_arrow_sink_layer::{OrderingLayer, UnorderedWriteLayer};
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
        target_file_size: Some(1),
        ..Default::default()
    }
}
fn count(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).unwrap()
}

async fn files(directory: &Path) -> Vec<std::path::PathBuf> {
    let mut entries = tokio::fs::read_dir(directory).await.unwrap();
    let mut result = Vec::new();
    while let Some(entry) = entries.next_entry().await.unwrap() {
        result.push(entry.path());
    }
    result.sort();
    result
}

async fn verify_rows(directory: &Path) {
    let mut rows = Vec::new();
    for path in files(directory).await {
        let file = tokio::fs::File::open(path).await.unwrap();
        let builder = ParquetRecordBatchStreamBuilder::new(file).await.unwrap();
        assert_eq!(builder.schema(), &batch().schema());
        let mut reader = builder.build().unwrap();
        while let Some(batch) = reader.try_next().await.unwrap() {
            let ids = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let text = batch
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            for i in 0..batch.num_rows() {
                rows.push((
                    (!ids.is_null(i)).then(|| ids.value(i)),
                    (!text.is_null(i)).then(|| text.value(i).to_owned()),
                ));
            }
        }
    }
    rows.sort();
    let mut expected = Vec::new();
    for _ in 0..12 {
        expected.extend([
            (Some(3), Some("雪".into())),
            (None, Some("".into())),
            (Some(1), None),
        ]);
    }
    expected.sort();
    assert_eq!(rows, expected);
}

#[tokio::test]
async fn preopens_files_and_round_trips_with_rolling() {
    let temp = tempfile::tempdir().unwrap();
    let directory = temp.path().join("output");
    let mut writer = ParquetDirectorySink
        .layer(UnorderedWriteLayer::new(count(3)))
        .open(directory.clone(), batch().schema(), options())
        .await
        .unwrap();
    assert_eq!(files(&directory).await.len(), 3);
    writer.write(batch().slice(0, 0)).await.unwrap();
    for _ in 0..12 {
        writer.write(batch()).await.unwrap();
    }
    assert_eq!(writer.commit().await.unwrap().rows_written, 36);
    assert!(files(&directory).await.len() > 3);
    verify_rows(&directory).await;
}

#[tokio::test]
async fn empty_commit_finishes_every_preopened_file() {
    let temp = tempfile::tempdir().unwrap();
    let directory = temp.path().join("output");
    let writer = ParquetDirectorySink
        .layer(UnorderedWriteLayer::new(count(3)))
        .open(directory.clone(), batch().schema(), options())
        .await
        .unwrap();
    assert_eq!(writer.commit().await.unwrap().rows_written, 0);
    assert_eq!(files(&directory).await.len(), 3);
    for path in files(&directory).await {
        let builder =
            ParquetRecordBatchStreamBuilder::new(tokio::fs::File::open(path).await.unwrap())
                .await
                .unwrap();
        assert_eq!(builder.schema(), &batch().schema());
        assert!(builder.build().unwrap().try_next().await.unwrap().is_none());
    }
}

#[tokio::test]
async fn abort_obeys_cleanup_policy_and_preserves_unrelated_files() {
    for cleanup in [false, true] {
        for foreign in [false, true] {
            let temp = tempfile::tempdir().unwrap();
            let directory = temp.path().join("output");
            let mut writer = ParquetDirectorySink
                .layer(UnorderedWriteLayer::new(count(3)))
                .open(
                    directory.clone(),
                    batch().schema(),
                    WriteOptions {
                        cleanup_on_error: cleanup,
                        ..options()
                    },
                )
                .await
                .unwrap();
            for _ in 0..9 {
                writer.write(batch()).await.unwrap();
            }
            if foreign {
                tokio::fs::write(directory.join("keep.txt"), b"keep")
                    .await
                    .unwrap();
            }
            writer.abort().await.unwrap();
            if foreign {
                assert_eq!(
                    tokio::fs::read(directory.join("keep.txt")).await.unwrap(),
                    b"keep"
                );
            }
            if cleanup {
                if foreign {
                    assert_eq!(files(&directory).await.len(), 1);
                } else {
                    assert!(!directory.exists());
                }
            } else {
                assert!(files(&directory).await.len() >= 3);
            }
        }
    }
}

#[tokio::test]
async fn rejects_existing_destination_and_preserve_order_without_side_effects() {
    let temp = tempfile::tempdir().unwrap();
    let directory = temp.path().join("output");
    let sink = ParquetDirectorySink.layer(UnorderedWriteLayer::new(count(3)));
    assert!(
        sink.open(directory.clone(), batch().schema(), WriteOptions::default())
            .await
            .is_err()
    );
    assert!(!directory.exists());
    tokio::fs::create_dir(&directory).await.unwrap();
    tokio::fs::write(directory.join("keep.txt"), b"keep")
        .await
        .unwrap();
    assert!(
        sink.open(directory.clone(), batch().schema(), options())
            .await
            .is_err()
    );
    assert_eq!(files(&directory).await.len(), 1);
}

#[tokio::test]
async fn ordering_layer_can_enable_unordered_but_cannot_force_preservation() {
    let temp = tempfile::tempdir().unwrap();
    let directory = temp.path().join("output");
    let sink = ParquetDirectorySink
        .layer(OrderingLayer::new(Ordering::Unordered))
        .layer(UnorderedWriteLayer::new(count(2)))
        .layer(OrderingLayer::new(Ordering::Unordered));
    let writer = sink
        .open(directory.clone(), batch().schema(), WriteOptions::default())
        .await
        .unwrap();
    writer.abort().await.unwrap();
    assert!(!directory.exists());
    let sink = ParquetDirectorySink
        .layer(OrderingLayer::new(Ordering::PreserveInput))
        .layer(UnorderedWriteLayer::new(count(2)));
    assert!(
        sink.open(directory.clone(), batch().schema(), options())
            .await
            .is_err()
    );
    assert!(!directory.exists());
}

#[tokio::test]
async fn failed_rolling_never_removes_a_conflicting_foreign_file() {
    use open_arrow_sink_core::ParallelSink;
    let temp = tempfile::tempdir().unwrap();
    let directory = temp.path().join("output");
    let mut writers = ParquetDirectorySink
        .open_writers(directory.clone(), batch().schema(), options(), count(2))
        .await
        .unwrap();
    let mut writer = writers.remove(0);
    writer.write(batch()).await.unwrap();
    let foreign = directory.join("part-00002.parquet");
    tokio::fs::write(&foreign, b"keep").await.unwrap();
    assert!(writer.write(batch()).await.is_err());
    writer.abort().await.unwrap();
    for writer in writers {
        writer.abort().await.unwrap();
    }
    assert_eq!(files(&directory).await, vec![foreign.clone()]);
    assert_eq!(tokio::fs::read(foreign).await.unwrap(), b"keep");
}

#[tokio::test]
async fn schema_failure_prevents_commit_and_cleans_all_files() {
    let temp = tempfile::tempdir().unwrap();
    let directory = temp.path().join("output");
    let mut writer = ParquetDirectorySink
        .layer(UnorderedWriteLayer::new(count(3)))
        .open(directory.clone(), batch().schema(), options())
        .await
        .unwrap();
    writer.write(batch()).await.unwrap();
    let wrong =
        RecordBatch::try_from_iter([("other", Arc::new(Int64Array::from(vec![1])) as ArrayRef)])
            .unwrap();
    assert!(writer.write(wrong).await.is_err());
    assert!(writer.commit().await.is_err());
    assert!(!directory.exists());
}
