use arrow_array::{ArrayRef, Int64Array, RecordBatch};
use open_arrow_sink_core::{IndexSpec, IndexType, Ordering, Sink, SinkExt, WriteOptions};
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

#[rstest::rstest]
#[case(false)]
#[case(true)]
#[tokio::test]
async fn ingested_blob_survives_source_removal(
    #[case] parallel: bool,
    #[values(false, true)] explicit_default_config: bool,
) {
    use lance::{
        blob::{BlobArrayBuilder, blob_field},
        datatypes::BlobHandling,
    };
    let storage = storage();
    let source = storage.url("source.bin").unwrap();
    storage.write(&source, "payload".into()).await.unwrap();
    let mut values = BlobArrayBuilder::new(1);
    values.push_uri(source.as_str()).unwrap();
    let schema = Arc::new(arrow_schema::Schema::new(vec![blob_field("blob", true)]));
    let batch = RecordBatch::try_new(schema.clone(), vec![values.finish().unwrap()]).unwrap();
    let sink = LanceDatasetSink::new(storage.clone());
    let options = WriteOptions {
        blob_handle: explicit_default_config.then(Default::default),
        ..write_options()
    };
    let url = storage.url("ingested/").unwrap();
    assert!(sink.capability().supports_blob_handle);
    let mut writer = if parallel {
        sink.clone()
            .layer(UnorderedWriteLayer::new(NonZeroUsize::new(2).unwrap()))
            .open(url.clone(), schema, options)
            .await
            .unwrap()
    } else {
        sink.open(url.clone(), schema, options).await.unwrap()
    };
    writer.write(batch).await.unwrap();
    writer.commit().await.unwrap();
    storage.delete(&source).await.unwrap();
    let dataset = sink.dataset_builder(url).unwrap().load().await.unwrap();
    let batch = dataset
        .scan()
        .blob_handling(BlobHandling::AllBinary)
        .try_into_batch()
        .await
        .unwrap();
    assert_eq!(
        batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::LargeBinaryArray>()
            .unwrap()
            .value(0),
        b"payload"
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
    let url = storage.url("versioned/").unwrap();
    let mut writer = sink
        .open(
            url.clone(),
            batch().schema(),
            WriteOptions {
                storage_version: Some("2.1".into()),
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
        "2.1"
    );
}

#[tokio::test]
async fn rejects_conflicting_blob_options_before_creating_destination() {
    use open_arrow_sink_core::{BlobHandleConfig, ExternalBlobMode};
    let storage = storage();
    let sink = LanceDatasetSink::new(storage.clone());
    let url = storage.url("invalid-blob/").unwrap();
    let result = sink
        .open(
            url.clone(),
            batch().schema(),
            WriteOptions {
                blob_handle: Some(BlobHandleConfig {
                    external_blob_mode: ExternalBlobMode::Ingest,
                    allow_external_blob_outside_bases: true,
                    ..Default::default()
                }),
                ..write_options()
            },
        )
        .await;
    assert!(matches!(
        result,
        Err(open_arrow_sink_core::Error::Unsupported(_))
    ));
    assert!(!storage.prefix_exists(&url).await.unwrap());
}
