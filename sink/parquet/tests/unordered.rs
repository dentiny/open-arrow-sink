use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use open_arrow_sink_core::{Sink, SinkExt};
use open_arrow_sink_layer::UnorderedWriteLayer;
use open_arrow_sink_parquet::ParquetDirectorySink;
use std::{num::NonZeroUsize, sync::Arc};

#[tokio::test]
async fn unordered_commit_preserves_all_rows() {
    let storage = open_arrow_sink_test_support::memory_storage("memory:///warehouse/");
    let directory = storage.url("雪 output/").unwrap();
    let make_batch = |id: i64| {
        RecordBatch::try_from_iter([
            (
                "id",
                Arc::new(Int64Array::from(vec![Some(id * 2), None, Some(id * 2 + 1)])) as ArrayRef,
            ),
            (
                "text",
                Arc::new(StringArray::from(vec![Some("雪"), Some(""), None])) as ArrayRef,
            ),
        ])
        .unwrap()
    };
    let mut writer = ParquetDirectorySink::new(storage.clone())
        .layer(UnorderedWriteLayer::new(NonZeroUsize::new(3).unwrap()))
        .open(
            directory.clone(),
            make_batch(0).schema(),
            open_arrow_sink_test_support::unordered_options().with_target_file_size(1),
        )
        .await
        .unwrap();
    let expected: Vec<_> = (0..8).map(make_batch).collect();
    for batch in &expected {
        writer.write(batch.clone()).await.unwrap();
    }
    assert_eq!(writer.commit().await.unwrap().rows_written, 24);
    let actual = open_arrow_sink_test_support::read_parquet_dir(
        &storage,
        &storage.resolve(&directory).unwrap(),
    )
    .await;
    assert_eq!(
        open_arrow_sink_test_support::concat_sorted(
            &make_batch(0).schema(),
            &actual,
            &["id", "text"]
        )
        .unwrap(),
        open_arrow_sink_test_support::concat_sorted(
            &make_batch(0).schema(),
            &expected,
            &["id", "text"]
        )
        .unwrap(),
    );
}

#[tokio::test]
async fn duplicate_create_preserves_existing_rows() {
    let storage = open_arrow_sink_test_support::memory_storage("memory:///warehouse/");
    let directory = storage.url("output/").unwrap();
    let batch = open_arrow_sink_test_support::int64_batch("id", vec![Some(1)]);
    let sink = ParquetDirectorySink::new(storage.clone());
    let mut writer = sink
        .open(
            directory.clone(),
            batch.schema(),
            open_arrow_sink_test_support::unordered_options(),
        )
        .await
        .unwrap();
    writer.write(batch.clone()).await.unwrap();
    writer.commit().await.unwrap();
    assert!(matches!(
        sink.open(
            directory.clone(),
            batch.schema(),
            open_arrow_sink_test_support::unordered_options()
        )
        .await,
        Err(open_arrow_sink_core::Error::AlreadyExists(_))
    ));
    let actual = open_arrow_sink_test_support::read_parquet_dir(
        &storage,
        &storage.resolve(&directory).unwrap(),
    )
    .await;
    assert_eq!(actual, vec![batch]);
}
