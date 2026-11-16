use arrow_array::RecordBatch;
use arrow_schema::Schema;
use lance::{
    blob::{BlobArrayBuilder, blob_field},
    datatypes::BlobKind,
};
use more_asserts::assert_gt;
use open_arrow_sink_core::{
    BlobHandleConfig, IndexSpec, IndexType, ParallelSink, Sink, SinkExt, UploadConfig, WriteOptions,
};
use open_arrow_sink_lance::{LanceDatasetSink, Storage};
use open_arrow_sink_layer::UnorderedWriteLayer;
use open_arrow_sink_test_support::write_batches;
use std::{num::NonZeroUsize, sync::Arc};

fn storage() -> Storage {
    open_arrow_sink_test_support::memory_storage("memory:///warehouse/")
}
fn batch() -> RecordBatch {
    open_arrow_sink_test_support::int64_batch("value", vec![Some(1), Some(2), Some(3)])
}
fn write_options() -> WriteOptions {
    open_arrow_sink_test_support::unordered_options().with_upload(
        UploadConfig::default()
            .with_part_size(NonZeroUsize::new(16 * 1024).unwrap())
            .with_concurrency(NonZeroUsize::new(2).unwrap()),
    )
}

struct IndexedDataset {
    sink: LanceDatasetSink,
    url: open_arrow_sink_lance::Url,
    io: open_arrow_sink_core::IoMetrics,
}
async fn indexed_dataset() -> &'static IndexedDataset {
    static DATASET: tokio::sync::OnceCell<IndexedDataset> = tokio::sync::OnceCell::const_new();
    DATASET
        .get_or_init(|| async {
            let storage = storage();
            let sink = LanceDatasetSink::new(storage.clone());
            let url = storage.url("indexed/").unwrap();
            let io = open_arrow_sink_core::IoMetrics::default();
            write_batches(
                &sink,
                url.clone(),
                &vec![batch(); 4],
                write_options()
                    .with_io_metrics(io.clone())
                    .with_indexes(vec![IndexSpec::new("value", IndexType::Scalar)]),
            )
            .await;
            IndexedDataset { sink, url, io }
        })
        .await
}

#[tokio::test]
async fn shared_operator_unordered_commit_preserves_rows() {
    let storage = storage();
    let sink = LanceDatasetSink::new(storage.clone());
    let url = storage.url("output/").unwrap();
    write_batches(
        &sink
            .clone()
            .layer(UnorderedWriteLayer::new(NonZeroUsize::new(3).unwrap())),
        url.clone(),
        &vec![batch(); 4],
        write_options(),
    )
    .await;
    let dataset = sink.dataset_builder(url).unwrap().load().await.unwrap();
    let actual = dataset.scan().try_into_batch().await.unwrap();
    let expected = vec![batch(); 4];
    assert_eq!(
        open_arrow_sink_test_support::concat_sorted(
            &actual.schema(),
            std::slice::from_ref(&actual),
            &["value"]
        )
        .unwrap(),
        open_arrow_sink_test_support::concat_sorted(&actual.schema(), &expected, &["value"])
            .unwrap(),
    );
}

#[tokio::test]
async fn shared_operator_records_read_and_write_metrics() {
    let fixture = indexed_dataset().await;
    let io = &fixture.io;
    assert_gt!(io.snapshot().bytes_read, 0);
    assert_gt!(io.snapshot().bytes_written, 0);
}

#[tokio::test]
async fn shared_operator_builds_requested_index() {
    use lance::index::DatasetIndexExt;
    let fixture = indexed_dataset().await;
    let dataset = fixture
        .sink
        .dataset_builder(fixture.url.clone())
        .unwrap()
        .load()
        .await
        .unwrap();
    assert_eq!(dataset.load_indices().await.unwrap().len(), 1);
}

#[tokio::test]
async fn native_files_use_the_configured_storage_namespace() {
    let storage = storage();
    let sink = LanceDatasetSink::new(storage.clone());
    let url = storage.url("雪 output%20/").unwrap();
    write_batches(&sink, url.clone(), &vec![batch(); 4], write_options()).await;
    assert_gt!(
        storage
            .list(&storage.resolve(&url).unwrap())
            .await
            .unwrap()
            .len(),
        1
    );
    let dataset = sink.dataset_builder(url).unwrap().load().await.unwrap();
    assert_eq!(dataset.count_rows(None).await.unwrap(), 12);
}

#[tokio::test]
async fn duplicate_create_preserves_the_existing_dataset() {
    let storage = storage();
    let sink = LanceDatasetSink::new(storage.clone());
    let url = storage.url("output/").unwrap();
    write_batches(&sink, url.clone(), &vec![batch(); 4], write_options()).await;
    assert!(matches!(
        sink.open(url.clone(), batch().schema(), write_options()).await,
        Err(open_arrow_sink_core::Error::AlreadyExists(location)) if location == url.as_str()
    ));
    let dataset = sink.dataset_builder(url).unwrap().load().await.unwrap();
    assert_eq!(dataset.count_rows(None).await.unwrap(), 12);
}

#[tokio::test]
async fn duplicate_index_reports_already_exists() {
    let fixture = indexed_dataset().await;
    let options = write_options().with_indexes(vec![IndexSpec::new("value", IndexType::Scalar)]);
    assert!(matches!(
        fixture.sink.open(fixture.url.clone(), batch().schema(), options.with_mode(open_arrow_sink_core::WriteMode::Append)).await,
        Err(open_arrow_sink_core::Error::AlreadyExists(message)) if message.contains("index oas_btree_")
    ));
}

#[tokio::test]
async fn shared_operator_abort_preserves_output() {
    let storage = storage();
    let sink = LanceDatasetSink::new(storage.clone());
    let url = storage.url("aborted/").unwrap();
    let mut session = sink
        .open_writers(
            url.clone(),
            batch().schema(),
            write_options(),
            NonZeroUsize::new(3).unwrap(),
        )
        .await
        .unwrap();
    session.writers[0].write(batch()).await.unwrap();
    session.writers[0].finish().await.unwrap();
    let directory = storage.resolve(&url).unwrap();
    let before = open_arrow_sink_test_support::sorted_list(&storage, &directory).await;
    assert!(!before.is_empty());
    let sibling = storage.url("keep").unwrap();
    storage
        .write(&storage.resolve(&sibling).unwrap(), "keep".into())
        .await
        .unwrap();
    for writer in session.writers {
        writer.abort().await.unwrap();
    }
    session.committer.abort().await.unwrap();
    let retained = open_arrow_sink_test_support::sorted_list(&storage, &directory).await;
    assert_eq!(retained, before);
    assert_eq!(
        storage
            .read(&storage.resolve(&sibling).unwrap())
            .await
            .unwrap(),
        "keep"
    );
}

#[tokio::test]
async fn explicit_storage_version() {
    let storage = storage();
    let sink = LanceDatasetSink::new(storage.clone());
    assert!(sink.capability().supports_storage_version);
    for version in ["2.2", "2.3"] {
        let url = storage.url(&format!("version-{version}/")).unwrap();
        let mut writer = sink
            .open(
                url.clone(),
                batch().schema(),
                write_options().with_storage_version(version.into()),
            )
            .await
            .unwrap();
        writer.write(batch()).await.unwrap();
        writer.commit().await.unwrap();
        let dataset = sink.dataset_builder(url).unwrap().load().await.unwrap();
        assert_eq!(
            dataset.manifest().data_storage_format.version.to_string(),
            version
        );
    }
    assert!(matches!(
        sink.open(
            storage.url("unsupported/").unwrap(),
            batch().schema(),
            write_options().with_storage_version("2.1".into())
        )
        .await,
        Err(open_arrow_sink_core::Error::Unsupported(_))
    ));
}

#[rstest::rstest]
#[case::inline("x", BlobKind::Inline, false)]
#[case::packed("xx", BlobKind::Packed, false)]
#[case::dedicated("xxx", BlobKind::Dedicated, false)]
#[case::parallel_dedicated("xxx", BlobKind::Dedicated, true)]
#[tokio::test]
async fn ingests_blob_with_configured_layout(
    #[case] payload: &str,
    #[case] kind: BlobKind,
    #[case] parallel: bool,
) {
    let storage = storage();
    let source = storage.url("source.bin").unwrap();
    storage
        .write(
            &storage.resolve(&source).unwrap(),
            payload.as_bytes().to_vec().into(),
        )
        .await
        .unwrap();
    let mut values = BlobArrayBuilder::new(1);
    values.push_uri(source.as_str()).unwrap();
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![blob_field("blob", true)])),
        vec![values.finish().unwrap()],
    )
    .unwrap();
    let options = write_options().with_blob_handle(BlobHandleConfig {
        inline_size_threshold: Some(1),
        dedicated_size_threshold: NonZeroUsize::new(2),
        ..Default::default()
    });
    let sink = LanceDatasetSink::new(storage.clone());
    let url = storage.url("ingested/").unwrap();
    let mut writer = if parallel {
        sink.clone()
            .layer(UnorderedWriteLayer::new(NonZeroUsize::new(2).unwrap()))
            .open(url.clone(), batch.schema(), options)
            .await
    } else {
        sink.open(url.clone(), batch.schema(), options).await
    }
    .unwrap();
    writer.write(batch).await.unwrap();
    writer.commit().await.unwrap();
    storage
        .delete(&storage.resolve(&source).unwrap())
        .await
        .unwrap();
    let dataset = sink.dataset_builder(url).unwrap().load().await.unwrap();
    let blobs = Arc::new(dataset)
        .take_blobs_by_indices(&[0], "blob")
        .await
        .unwrap();
    let blob = blobs[0].as_ref().unwrap();
    assert_eq!(blob.kind(), kind);
    assert_eq!(blob.read().await.unwrap().as_ref(), payload.as_bytes());
}

#[tokio::test]
async fn external_blobs_resolve_independent_buckets() {
    let make = |base: &str| open_arrow_sink_test_support::memory_storage(base);
    let destination = make("s3://destination/");
    let first = make("s3://source-a/");
    let second = make("s3://source-b/");
    let shadow = make("s3://destination/local/");
    shadow
        .write(
            &open_arrow_sink_test_support::path(&shadow, "blob"),
            b"wrong".to_vec().into(),
        )
        .await
        .unwrap();
    let owned = destination.url("local/blob").unwrap();
    destination
        .write(
            &destination.resolve(&owned).unwrap(),
            b"owned".to_vec().into(),
        )
        .await
        .unwrap();
    let mut values = BlobArrayBuilder::new(3);
    for (storage, bytes) in [(&first, "first"), (&second, "second")] {
        let uri = storage.url("same-key").unwrap();
        storage
            .write(
                &storage.resolve(&uri).unwrap(),
                bytes.as_bytes().to_vec().into(),
            )
            .await
            .unwrap();
        values.push_uri(uri.as_str()).unwrap();
    }
    values.push_uri(owned.as_str()).unwrap();
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![blob_field("blob", true)])),
        vec![values.finish().unwrap()],
    )
    .unwrap();
    let downloader = open_arrow_sink_storage::BlobDownloader::with_defaults()
        .unwrap()
        .with_source_storage(first.clone())
        .with_source_storage(second.clone())
        .with_source_storage(shadow);
    let sink =
        LanceDatasetSink::new(destination.clone()).with_blob_downloader(Arc::new(downloader));
    let url = destination.url("dataset/").unwrap();
    let mut writer = sink
        .open(url.clone(), batch.schema(), write_options())
        .await
        .unwrap();
    writer.write(batch).await.unwrap();
    writer.commit().await.unwrap();
    first
        .delete(&open_arrow_sink_test_support::path(&first, "same-key"))
        .await
        .unwrap();
    second
        .delete(&open_arrow_sink_test_support::path(&second, "same-key"))
        .await
        .unwrap();
    destination
        .delete(&destination.resolve(&owned).unwrap())
        .await
        .unwrap();
    let dataset = Arc::new(sink.dataset_builder(url).unwrap().load().await.unwrap());
    let blobs = dataset
        .take_blobs_by_indices(&[0, 1, 2], "blob")
        .await
        .unwrap();
    let mut actual = Vec::new();
    for blob in blobs {
        actual.push(blob.unwrap().read().await.unwrap().to_vec());
    }
    actual.sort();
    assert_eq!(
        actual,
        vec![b"first".to_vec(), b"owned".to_vec(), b"second".to_vec()]
    );
}

#[tokio::test]
async fn io_metrics_are_isolated_per_session() {
    let store = storage();
    let sink = LanceDatasetSink::new(store.clone());
    let mut totals = Vec::new();
    for name in ["first", "second"] {
        let io = open_arrow_sink_core::IoMetrics::default();
        let mut writer = sink
            .open(
                store.url(name).unwrap(),
                batch().schema(),
                // Building the index forces reads within the measured session.
                write_options()
                    .with_io_metrics(io.clone())
                    .with_indexes(vec![IndexSpec::new("value", IndexType::Scalar)]),
            )
            .await
            .unwrap();
        writer.write(batch()).await.unwrap();
        writer.commit().await.unwrap();
        totals.push(io.snapshot().bytes_written);
    }
    assert!(totals[0] > 0);
    // Identical output shapes should have comparable counters, not accumulate.
    assert!(totals[1] < totals[0] * 3 / 2);
}
