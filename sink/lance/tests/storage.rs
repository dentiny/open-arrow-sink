use arrow_array::{ArrayRef, Int64Array, RecordBatch};
use arrow_schema::Schema;
use lance::{
    blob::{BlobArrayBuilder, blob_field},
    datatypes::BlobKind,
};
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
    let options = WriteOptions {
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
    assert_eq!(writer.commit().await.unwrap().rows_written, 12);
    let dataset = sink
        .dataset_builder(url.clone())
        .unwrap()
        .load()
        .await
        .unwrap();
    let actual = dataset.scan().try_into_batch().await.unwrap();
    let mut values = actual
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .values()
        .to_vec();
    values.sort_unstable();
    assert_eq!(values, vec![1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3]);
    assert_eq!(dataset.load_indices().await.unwrap().len(), 1);
    // Data and the reservation must resolve to the same physical namespace.
    assert!(storage.list(&url).await.unwrap().len() > 1);
    assert!(
        sink.open(url, batch().schema(), write_options())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn shared_operator_abort_honors_cleanup() {
    for cleanup_on_error in [true, false] {
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
#[case::inline("x", BlobKind::Inline)]
#[case::packed("xx", BlobKind::Packed)]
#[case::dedicated("xxx", BlobKind::Dedicated)]
#[tokio::test]
async fn ingests_blob_with_configured_layout(#[case] payload: &str, #[case] kind: BlobKind) {
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
    let mut writer = sink
        .open(url.clone(), batch.schema(), options)
        .await
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
