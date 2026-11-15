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

#[rstest::rstest]
#[tokio::test]
async fn blob_thresholds_select_inline_packed_and_dedicated(
    #[values(false, true)] parallel: bool,
    #[values(false, true)] from_uri: bool,
) {
    use lance::{
        blob::{BlobArrayBuilder, BlobFieldOptions, blob_field_with_options},
        datatypes::BlobKind,
    };
    use open_arrow_sink_core::BlobHandleConfig;
    let storage = storage();
    let sink = LanceDatasetSink::new(storage.clone());
    let sizes = [4, 5, 8, 9];
    let mut sources = Vec::new();
    let mut values = BlobArrayBuilder::new(sizes.len());
    for size in sizes {
        let bytes = vec![b'x'; size];
        if from_uri {
            let source = storage.url(&format!("source-{size}.bin")).unwrap();
            storage.write(&source, bytes.into()).await.unwrap();
            values.push_uri(source.as_str()).unwrap();
            sources.push(source);
        } else {
            values.push_bytes(&bytes).unwrap();
        }
    }
    // Explicit write config overrides existing per-field thresholds.
    let field = blob_field_with_options(
        "blob",
        true,
        BlobFieldOptions {
            inline_size_threshold: Some(100),
            dedicated_size_threshold: NonZeroUsize::new(200),
        },
    );
    let schema = Arc::new(arrow_schema::Schema::new(vec![field]));
    let batch = RecordBatch::try_new(schema.clone(), vec![values.finish().unwrap()]).unwrap();
    let options = WriteOptions {
        blob_handle: Some(BlobHandleConfig {
            inline_size_threshold: Some(4),
            dedicated_size_threshold: NonZeroUsize::new(8),
            ..Default::default()
        }),
        ..write_options()
    };
    let url = storage.url("thresholds/").unwrap();
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
    for source in sources {
        storage.delete(&source).await.unwrap();
    }
    let dataset = sink.dataset_builder(url).unwrap().load().await.unwrap();
    let blobs = Arc::new(dataset)
        .take_blobs_by_indices(&[0, 1, 2, 3], "blob")
        .await
        .unwrap();
    let kinds = [
        BlobKind::Inline,
        BlobKind::Packed,
        BlobKind::Packed,
        BlobKind::Dedicated,
    ];
    for ((blob, size), kind) in blobs.into_iter().zip(sizes).zip(kinds) {
        let blob = blob.unwrap();
        assert_eq!(blob.kind(), kind);
        assert_eq!(blob.read().await.unwrap().as_ref(), vec![b'x'; size]);
    }
}
