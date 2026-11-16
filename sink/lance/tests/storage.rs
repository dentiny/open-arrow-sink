use arrow_array::cast::AsArray;
use arrow_array::{ArrayRef, Int64Array, RecordBatch};
use arrow_schema::Schema;
use lance::{
    blob::{BlobArrayBuilder, blob_field},
    datatypes::BlobKind,
};
use more_asserts::assert_gt;
use open_arrow_sink_core::{
    BlobHandleConfig, IndexSpec, IndexType, Ordering, Sink, SinkExt, WriteOptions,
};
use open_arrow_sink_lance::{LanceDatasetSink, Storage};
use open_arrow_sink_layer::UnorderedWriteLayer;
use open_arrow_sink_storage::opendal;
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
fn write_options() -> WriteOptions {
    WriteOptions {
        ordering: Ordering::Unordered,
        ..Default::default()
    }
}

#[tokio::test]
async fn shared_operator_commit_indexes_and_read_back() {
    use lance::index::DatasetIndexExt;
    let storage = storage();
    let sink = LanceDatasetSink::new(storage.clone());
    let url = storage.url("雪 output%20/").unwrap();
    let io = open_arrow_sink_core::IoMetrics::default();
    let options = WriteOptions {
        io_metrics: Some(io.clone()),
        indexes: vec![IndexSpec::new("value", IndexType::Scalar)],
        ..write_options()
    };
    let mut writer = sink
        .clone()
        .layer(UnorderedWriteLayer::new(NonZeroUsize::new(3).unwrap()))
        .open(url.clone(), batch().schema(), options)
        .await
        .unwrap();
    for _ in 0..4 {
        writer.write(batch()).await.unwrap();
    }
    let summary = writer.commit().await.unwrap();
    assert_eq!(summary.rows_written, 12);
    assert_gt!(io.snapshot().bytes_read, 0);
    assert_gt!(io.snapshot().bytes_written, 0);
    let dataset = sink
        .dataset_builder(url.clone())
        .unwrap()
        .load()
        .await
        .unwrap();
    let actual = dataset.scan().try_into_batch().await.unwrap();
    let mut values = actual
        .column(0)
        .as_primitive::<arrow_array::types::Int64Type>()
        .values()
        .to_vec();
    values.sort_unstable();
    assert_eq!(values, vec![1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3]);
    assert_eq!(dataset.load_indices().await.unwrap().len(), 1);
    // Data and the reservation must resolve to the same physical namespace.
    assert_gt!(storage.list(&url).await.unwrap().len(), 1);
    assert!(
        sink.open(url, batch().schema(), write_options())
            .await
            .is_err()
    );
}

#[rstest::rstest]
#[case::cleanup(true)]
#[case::keep(false)]
#[tokio::test]
async fn shared_operator_abort_honors_cleanup(#[case] cleanup_on_error: bool) {
    let storage = storage();
    let sink = LanceDatasetSink::new(storage.clone())
        .layer(UnorderedWriteLayer::new(NonZeroUsize::new(3).unwrap()));
    let url = storage.url("aborted/").unwrap();
    let mut writer = sink
        .open(
            url.clone(),
            batch().schema(),
            WriteOptions {
                cleanup_on_error,
                ..write_options()
            },
        )
        .await
        .unwrap();
    writer.write(batch()).await.unwrap();
    let sibling = storage.url("keep").unwrap();
    storage.write(&sibling, "keep".into()).await.unwrap();
    writer.abort().await.unwrap();
    assert_eq!(
        storage.list(&url).await.unwrap().is_empty(),
        cleanup_on_error
    );
    assert_eq!(storage.read(&sibling).await.unwrap(), "keep");
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
                WriteOptions {
                    storage_version: Some(version.into()),
                    ..write_options()
                },
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
            WriteOptions {
                storage_version: Some("2.1".into()),
                ..write_options()
            }
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
        .write(&source, payload.as_bytes().to_vec().into())
        .await
        .unwrap();
    let mut values = BlobArrayBuilder::new(1);
    values.push_uri(source.as_str()).unwrap();
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![blob_field("blob", true)])),
        vec![values.finish().unwrap()],
    )
    .unwrap();
    let options = WriteOptions {
        blob_handle: Some(BlobHandleConfig {
            inline_size_threshold: Some(1),
            dedicated_size_threshold: NonZeroUsize::new(2),
            ..Default::default()
        }),
        ..write_options()
    };
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
    storage.delete(&source).await.unwrap();
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
    let make = |base: &str| {
        Storage::new(
            opendal::Operator::new(opendal::services::Memory::default()).unwrap(),
            base.parse().unwrap(),
        )
        .unwrap()
    };
    let destination = make("s3://destination/");
    let first = make("s3://source-a/");
    let second = make("s3://source-b/");
    let mut values = BlobArrayBuilder::new(2);
    for (storage, bytes) in [(&first, "first"), (&second, "second")] {
        let uri = storage.url("same-key").unwrap();
        storage
            .write(&uri, bytes.as_bytes().to_vec().into())
            .await
            .unwrap();
        values.push_uri(uri.as_str()).unwrap();
    }
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![blob_field("blob", true)])),
        vec![values.finish().unwrap()],
    )
    .unwrap();
    let sink = LanceDatasetSink::new(destination.clone())
        .with_source_storage(first.clone())
        .with_source_storage(second.clone());
    let url = destination.url("dataset/").unwrap();
    let mut writer = sink
        .open(url.clone(), batch.schema(), write_options())
        .await
        .unwrap();
    writer.write(batch).await.unwrap();
    writer.commit().await.unwrap();
    first.delete(&first.url("same-key").unwrap()).await.unwrap();
    second
        .delete(&second.url("same-key").unwrap())
        .await
        .unwrap();
    let dataset = Arc::new(sink.dataset_builder(url).unwrap().load().await.unwrap());
    let blobs = dataset
        .take_blobs_by_indices(&[0, 1], "blob")
        .await
        .unwrap();
    let mut actual = Vec::new();
    for blob in blobs {
        actual.push(blob.unwrap().read().await.unwrap().to_vec());
    }
    actual.sort();
    assert_eq!(actual, vec![b"first".to_vec(), b"second".to_vec()]);
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
                WriteOptions {
                    io_metrics: Some(io.clone()),
                    ..write_options()
                },
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
