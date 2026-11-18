use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use open_arrow_sink_core::{Sink, SinkExt};
use open_arrow_sink_layer::UnorderedWriteLayer;
use open_arrow_sink_parquet::ParquetDirectorySink;
use open_arrow_sink_test_support::url as fixture_url;
use open_arrow_sink_test_support::write_batches;
use std::{num::NonZeroUsize, sync::Arc};

#[tokio::test]
async fn unordered_commit_preserves_all_rows() {
    let storage = open_arrow_sink_test_support::memory_warehouse();
    let directory = fixture_url(&storage, "雪 output/");
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
    let sink = ParquetDirectorySink::new(storage.clone())
        .layer(UnorderedWriteLayer::new(NonZeroUsize::new(3).unwrap()));
    let expected: Vec<_> = (0..8).map(make_batch).collect();
    write_batches(
        &sink,
        directory.clone(),
        &expected,
        open_arrow_sink_test_support::unordered_options().with_target_file_size(1),
    )
    .await;
    let actual = open_arrow_sink_test_support::read_parquet_dir(
        &storage,
        &storage.path(&directory).unwrap(),
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
    let storage = open_arrow_sink_test_support::memory_warehouse();
    let directory = fixture_url(&storage, "output/");
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
        &storage.path(&directory).unwrap(),
    )
    .await;
    assert_eq!(actual, vec![batch]);
}

#[tokio::test]
async fn automatic_ingestion_streams_and_rotates_using_its_target() {
    use futures::stream;
    use open_arrow_sink_layer::{InputSizeHint, ParallelIngestionLayer};

    let storage = open_arrow_sink_test_support::memory_warehouse();
    let directory = fixture_url(&storage, "automatic/");
    let policy = ParallelIngestionLayer::new(NonZeroUsize::MIN)
        .with_input_size(InputSizeHint::SourceBytes(100))
        .with_max_writers(NonZeroUsize::new(2).unwrap());
    let sink = ParquetDirectorySink::new(storage.clone()).layer(policy);
    assert_eq!(sink.plan().writers.get(), 2);
    let batches: Vec<_> = (0..6)
        .map(|id| open_arrow_sink_test_support::int64_batch("id", vec![Some(id)]))
        .collect();
    // No target in WriteOptions: the layer forwards its target to file rotation.
    let summary = sink
        .write_all(
            directory.clone(),
            batches[0].schema(),
            open_arrow_sink_test_support::unordered_options(),
            stream::iter(batches.iter().cloned().map(Ok)),
        )
        .await
        .unwrap();
    assert_eq!(summary.rows_committed, 6);
    let path = storage.path(&directory).unwrap();
    let files = open_arrow_sink_test_support::sorted_list(&storage, &path).await;
    // A one-byte soft target rotates each batch into a new file, independently
    // of the two-writer concurrency limit.
    assert_eq!(files.len(), 6);
    let actual = open_arrow_sink_test_support::read_parquet_dir(&storage, &path).await;
    assert_eq!(
        open_arrow_sink_test_support::concat_sorted(&batches[0].schema(), &actual, &["id"])
            .unwrap(),
        open_arrow_sink_test_support::concat_sorted(&batches[0].schema(), &batches, &["id"])
            .unwrap(),
    );
}
