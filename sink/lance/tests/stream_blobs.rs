use std::{num::NonZeroUsize, sync::Arc};

use arrow_array::{Array, ArrayRef, LargeBinaryArray, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use lance::datatypes::BlobHandling;
use open_arrow_sink_core::{
    BLOB_METADATA_KEY, BlobChunk, BlobPlacement, Compression, Ordering, Sink, SinkExt, WriteOptions,
};
use open_arrow_sink_lance::{LanceDatasetSink, Storage};
use open_arrow_sink_layer::UnorderedWriteLayer;
use open_arrow_sink_storage::opendal;

fn storage() -> Storage {
    Storage::new(
        opendal::Operator::new(opendal::services::Memory::default()).unwrap(),
        "memory:///warehouse/".parse().unwrap(),
    )
    .unwrap()
}
fn input() -> RecordBatch {
    let field = Field::new("blob", DataType::LargeBinary, true)
        .with_metadata([(BLOB_METADATA_KEY.into(), "true".into())].into());
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![field])),
        vec![Arc::new(LargeBinaryArray::from(vec![
            Some(b"".as_slice()),
            None,
            Some(b"".as_slice()),
        ])) as ArrayRef],
    )
    .unwrap()
}
fn options() -> WriteOptions {
    WriteOptions {
        ordering: Ordering::Unordered,
        ..Default::default()
    }
}

#[tokio::test]
async fn shared_storage_streams_large_empty_and_null_blobs() {
    let storage = storage();
    let url = storage.url("雪 output%20/").unwrap();
    let sink = LanceDatasetSink::new(storage);
    let mut options = options();
    options.compression.global = Some(Compression::Zstd { level: Some(3) });
    let mut writer = sink
        .open(url.clone(), input().schema(), options)
        .await
        .unwrap();
    assert!(writer.supports_blob_streams());
    let mut blob = writer.create_blob().await.unwrap();
    // A 17 MiB value streamed from bounded chunks.
    for _ in 0..272 {
        blob.write(BlobChunk::new(vec![b'x'; 64 * 1024]))
            .await
            .unwrap();
    }
    let large = blob.finish().await.unwrap();
    let empty = writer.create_blob().await.unwrap().finish().await.unwrap();
    // Unattached finished blobs must not survive a successful commit.
    drop(writer.create_blob().await.unwrap().finish().await.unwrap());
    writer
        .write_with_blobs(
            input(),
            vec![
                BlobPlacement {
                    column: 0,
                    row: 0,
                    blob: large,
                },
                BlobPlacement {
                    column: 0,
                    row: 2,
                    blob: empty,
                },
            ],
        )
        .await
        .unwrap();
    assert_eq!(writer.commit().await.unwrap().rows_written, 3);
    let dataset = sink.dataset_builder(url).unwrap().load().await.unwrap();
    let actual = dataset
        .scan()
        .blob_handling(BlobHandling::AllBinary)
        .try_into_batch()
        .await
        .unwrap();
    let values = actual
        .column(0)
        .as_any()
        .downcast_ref::<LargeBinaryArray>()
        .unwrap();
    assert_eq!(values.value(0).len(), 17 * 1024 * 1024);
    assert!(values.value(0).iter().all(|&b| b == b'x'));
    assert!(values.is_null(1));
    assert!(!values.is_null(2));
    assert_eq!(values.value(2), b"");
}

#[tokio::test]
async fn configured_filesystem_root_preserves_marked_and_ordinary_binary() {
    let temp = tempfile::tempdir().unwrap();
    let storage = Storage::new(
        opendal::Operator::new(
            opendal::services::Fs::default().root(temp.path().to_str().unwrap()),
        )
        .unwrap(),
        "file:///warehouse/".parse().unwrap(),
    )
    .unwrap();
    let url = storage.url("dataset/").unwrap();
    let sink = LanceDatasetSink::new(storage);
    let payload = vec![0xab; 128 * 1024];
    let values = Arc::new(LargeBinaryArray::from(vec![
        Some(payload.as_slice()),
        None,
        Some(b"".as_slice()),
    ]));
    let schema = Arc::new(Schema::new(vec![
        input().schema().field(0).clone(),
        Field::new("ordinary", DataType::LargeBinary, true),
    ]));
    let batch = RecordBatch::try_new(schema.clone(), vec![values.clone(), values]).unwrap();
    let mut writer = sink
        .open(url.clone(), schema.clone(), options())
        .await
        .unwrap();
    assert_eq!(writer.schema(), &schema);
    writer.write(batch.clone()).await.unwrap();
    writer.commit().await.unwrap();
    let dataset = sink.dataset_builder(url).unwrap().load().await.unwrap();
    let stored = Schema::from(dataset.schema());
    assert_eq!(
        stored.field(0).metadata()["ARROW:extension:name"],
        "lance.blob.v2"
    );
    assert_eq!(stored.field(1).data_type(), &DataType::LargeBinary);
    let actual = dataset
        .scan()
        .blob_handling(BlobHandling::AllBinary)
        .try_into_batch()
        .await
        .unwrap();
    for column in 0..2 {
        assert_eq!(actual.column(column), batch.column(column));
    }
}

#[tokio::test]
async fn partial_or_unfinished_blobs_prevent_publication_and_are_cleaned_up() {
    for drop_partial in [true, false] {
        let storage = storage();
        let url = storage.url("failed/").unwrap();
        let sink = LanceDatasetSink::new(storage.clone());
        let mut writer = sink
            .open(url.clone(), input().schema(), options())
            .await
            .unwrap();
        let mut blob = Some(writer.create_blob().await.unwrap());
        blob.as_mut()
            .unwrap()
            .write(BlobChunk::new("partial"))
            .await
            .unwrap();
        if drop_partial {
            drop(blob.take());
        }
        assert!(writer.commit().await.is_err());
        assert!(storage.list(&url).await.unwrap().is_empty());
        if let Some(mut blob) = blob {
            assert!(blob.write(BlobChunk::new("late")).await.is_err());
            assert!(blob.finish().await.is_err());
        }
    }
}

#[tokio::test]
async fn rejects_handles_from_another_session() {
    let storage = storage();
    let sink = LanceDatasetSink::new(storage.clone());
    let mut first = sink
        .open(storage.url("first/").unwrap(), input().schema(), options())
        .await
        .unwrap();
    let second_url = storage.url("second/").unwrap();
    let mut second = sink
        .open(second_url.clone(), input().schema(), options())
        .await
        .unwrap();
    let blob = first.create_blob().await.unwrap().finish().await.unwrap();
    assert!(
        second
            .write_with_blobs(
                input(),
                vec![BlobPlacement {
                    column: 0,
                    row: 0,
                    blob
                }]
            )
            .await
            .is_err()
    );
    assert!(second.commit().await.is_err());
    assert!(storage.list(&second_url).await.unwrap().is_empty());
    first.abort().await.unwrap();
}

#[tokio::test]
async fn rejects_duplicate_and_null_placements() {
    for rows in [[0, 0], [0, 1]] {
        let storage = storage();
        let url = storage.url("invalid/").unwrap();
        let mut writer = LanceDatasetSink::new(storage.clone())
            .open(url.clone(), input().schema(), options())
            .await
            .unwrap();
        let a = writer.create_blob().await.unwrap().finish().await.unwrap();
        let b = writer.create_blob().await.unwrap().finish().await.unwrap();
        assert!(
            writer
                .write_with_blobs(
                    input(),
                    vec![
                        BlobPlacement {
                            column: 0,
                            row: rows[0],
                            blob: a
                        },
                        BlobPlacement {
                            column: 0,
                            row: rows[1],
                            blob: b
                        },
                    ]
                )
                .await
                .is_err()
        );
        assert!(writer.commit().await.is_err());
        assert!(storage.list(&url).await.unwrap().is_empty());
    }
}

#[tokio::test]
async fn invalid_or_parallel_blob_schemas_fail_before_destination_io() {
    let storage = storage();
    let url = storage.url("invalid/").unwrap();
    let sink = LanceDatasetSink::new(storage.clone());
    let invalid = Arc::new(Schema::new(vec![
        Field::new("blob", DataType::Utf8, true)
            .with_metadata([(BLOB_METADATA_KEY.into(), "true".into())].into()),
    ]));
    assert!(sink.open(url.clone(), invalid, options()).await.is_err());
    assert!(
        sink.layer(UnorderedWriteLayer::new(NonZeroUsize::new(2).unwrap()))
            .open(url.clone(), input().schema(), options())
            .await
            .is_err()
    );
    assert!(storage.list(&url).await.unwrap().is_empty());
}
