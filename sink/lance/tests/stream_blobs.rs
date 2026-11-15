use std::sync::Arc;

use arrow_array::{Array, ArrayRef, LargeBinaryArray, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use lance::{Dataset, datatypes::BlobHandling};
use open_arrow_sink_core::{
    BLOB_METADATA_KEY, BlobChunk, BlobPlacement, Compression, Ordering, Sink, WriteOptions,
};
use open_arrow_sink_lance::LanceDatasetSink;

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

#[rstest::rstest]
#[case(None)]
#[case(Some(Compression::Zstd { level: Some(3) }))]
#[tokio::test]
async fn streams_native_blobs_and_preserves_empty_and_null(
    #[case] compression: Option<Compression>,
) {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("dataset");
    let input = input();
    let mut options = options();
    options.compression.global = compression;
    let mut writer = LanceDatasetSink
        .open(path.clone(), input.schema(), options)
        .await
        .unwrap();
    assert!(writer.supports_blob_streams());
    let mut blob = writer.create_blob().await.unwrap();
    // A 17 MiB value assembled from small chunks, without a complete payload buffer.
    for _ in 0..272 {
        blob.write(BlobChunk::new(vec![b'x'; 64 * 1024]))
            .await
            .unwrap();
    }
    let large = blob.finish().await.unwrap();
    let empty = writer.create_blob().await.unwrap().finish().await.unwrap();
    writer
        .write_with_blobs(
            input,
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
    let dataset = Dataset::open(path.to_str().unwrap()).await.unwrap();
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
async fn dropping_partial_blob_poison_session_and_commit_cleans_up() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("dataset");
    let mut writer = LanceDatasetSink
        .open(path.clone(), input().schema(), options())
        .await
        .unwrap();
    let mut blob = writer.create_blob().await.unwrap();
    blob.write(BlobChunk::new(vec![1; 1024])).await.unwrap();
    drop(blob);
    assert!(writer.commit().await.is_err());
    assert!(!path.exists());
}

#[tokio::test]
async fn rejects_handles_from_another_session() {
    let temp = tempfile::tempdir().unwrap();
    let first_path = temp.path().join("first");
    let second_path = temp.path().join("second");
    let input = input();
    let mut first = LanceDatasetSink
        .open(first_path.clone(), input.schema(), options())
        .await
        .unwrap();
    let mut second = LanceDatasetSink
        .open(second_path.clone(), input.schema(), options())
        .await
        .unwrap();
    let blob = first.create_blob().await.unwrap().finish().await.unwrap();
    assert!(
        second
            .write_with_blobs(
                input,
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
    assert!(!second_path.exists());
    first.abort().await.unwrap();
}

#[tokio::test]
async fn rejects_duplicate_and_null_placements() {
    for rows in [[0, 0], [0, 1]] {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("dataset");
        let input = input();
        let mut writer = LanceDatasetSink
            .open(path.clone(), input.schema(), options())
            .await
            .unwrap();
        let a = writer.create_blob().await.unwrap().finish().await.unwrap();
        let b = writer.create_blob().await.unwrap().finish().await.unwrap();
        assert!(
            writer
                .write_with_blobs(
                    input,
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
        assert!(!path.exists());
    }
}

#[tokio::test]
async fn ordinary_materialized_blob_values_still_use_native_storage() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("dataset");
    let schema = input().schema();
    let values =
        LargeBinaryArray::from(vec![Some(b"hello".as_slice()), None, Some(b"".as_slice())]);
    let batch = RecordBatch::try_new(schema, vec![Arc::new(values)]).unwrap();
    let mut writer = LanceDatasetSink
        .open(path.clone(), batch.schema(), options())
        .await
        .unwrap();
    writer.write(batch).await.unwrap();
    writer.commit().await.unwrap();
    let dataset = Dataset::open(path.to_str().unwrap()).await.unwrap();
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
    assert_eq!(values.value(0), b"hello");
    assert!(values.is_null(1));
    assert_eq!(values.value(2), b"");
}

#[tokio::test]
async fn abort_invalidates_live_blob_writers() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("dataset");
    let mut writer = LanceDatasetSink
        .open(path.clone(), input().schema(), options())
        .await
        .unwrap();
    let mut blob = writer.create_blob().await.unwrap();
    writer.abort().await.unwrap();
    assert!(blob.write(BlobChunk::new(vec![1; 8])).await.is_err());
    assert!(blob.finish().await.is_err());
    assert!(!path.exists());
}

#[tokio::test]
async fn marked_binary_uses_native_blob_v2_and_round_trips_null_empty_and_payload() {
    use arrow_array::LargeBinaryArray;
    use arrow_schema::{DataType, Field, Schema};
    use lance::datatypes::BlobHandling;
    use open_arrow_sink_core::BLOB_METADATA_KEY;

    let payload = vec![0xab; 128 * 1024];
    let values = Arc::new(LargeBinaryArray::from(vec![
        Some(payload.as_slice()),
        None,
        Some(b"".as_slice()),
    ]));
    let schema = Arc::new(Schema::new(vec![
        Field::new("blob", DataType::LargeBinary, true)
            .with_metadata([(BLOB_METADATA_KEY.into(), "true".into())].into()),
        Field::new("ordinary_binary", DataType::LargeBinary, true),
    ]));
    let batch = RecordBatch::try_new(schema.clone(), vec![values.clone(), values]).unwrap();
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("blobs.lance");
    let mut writer = LanceDatasetSink
        .open(path.clone(), schema.clone(), options())
        .await
        .unwrap();
    assert_eq!(writer.schema(), &schema);
    writer.write(batch.clone()).await.unwrap();
    assert_eq!(writer.commit().await.unwrap().rows_written, 3);
    let dataset = Dataset::open(path.to_str().unwrap()).await.unwrap();
    let native_schema = arrow_schema::Schema::from(dataset.schema());
    assert_eq!(
        native_schema.field(0).metadata()["ARROW:extension:name"],
        "lance.blob.v2"
    );
    assert!(matches!(
        native_schema.field(0).data_type(),
        DataType::Struct(_)
    ));
    assert_eq!(native_schema.field(1).data_type(), &DataType::LargeBinary);
    let actual = dataset
        .scan()
        .blob_handling(BlobHandling::AllBinary)
        .try_into_batch()
        .await
        .unwrap();
    for index in 0..2 {
        assert_eq!(actual.column(index), batch.column(index));
    }
}

#[tokio::test]
async fn invalid_blob_marker_is_rejected_before_creating_destination() {
    use arrow_schema::{DataType, Field, Schema};
    use open_arrow_sink_core::BLOB_METADATA_KEY;
    let schema = Arc::new(Schema::new(vec![
        Field::new("blob", DataType::Utf8, true)
            .with_metadata([(BLOB_METADATA_KEY.into(), "true".into())].into()),
    ]));
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("invalid.lance");
    assert!(
        LanceDatasetSink
            .open(path.clone(), schema, options())
            .await
            .is_err()
    );
    assert!(!path.exists());
}
