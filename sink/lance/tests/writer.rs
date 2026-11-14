use std::sync::Arc;

use arrow_array::{Array, ArrayRef, Int64Array, RecordBatch};
use lance::Dataset;
use open_arrow_sink_core::{Ordering, Sink, WriteOptions};
use open_arrow_sink_lance::LanceDatasetSink;

fn batch() -> RecordBatch {
    RecordBatch::try_from_iter([(
        "value",
        Arc::new(Int64Array::from(vec![Some(3), None, Some(1), Some(3)])) as ArrayRef,
    )])
    .unwrap()
}

fn options() -> WriteOptions {
    WriteOptions {
        ordering: Ordering::Unordered,
        ..WriteOptions::default()
    }
}

#[tokio::test]
async fn commit_round_trips_multiple_batches() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("dataset");
    let expected = batch();
    let mut writer = LanceDatasetSink
        .open(path.clone(), expected.schema(), options())
        .await
        .unwrap();
    assert_eq!(writer.schema(), &expected.schema());
    writer.write(expected.slice(0, 1)).await.unwrap();
    writer.write(expected.slice(1, 3)).await.unwrap();
    assert_eq!(writer.commit().await.unwrap().rows_written, 4);
    let dataset = Dataset::open(path.to_str().unwrap()).await.unwrap();
    let actual = dataset.scan().try_into_batch().await.unwrap();
    assert_eq!(actual.schema(), expected.schema());
    let values = actual
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let mut values: Vec<_> = (0..values.len())
        .map(|i| (!values.is_null(i)).then(|| values.value(i)))
        .collect();
    values.sort_unstable();
    assert_eq!(values, vec![None, Some(1), Some(3), Some(3)]);
}

#[tokio::test]
async fn empty_commit_produces_readable_dataset() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("dataset");
    let schema = batch().schema();
    let writer = LanceDatasetSink
        .open(path.clone(), schema.clone(), options())
        .await
        .unwrap();
    assert_eq!(writer.commit().await.unwrap().rows_written, 0);
    let dataset = Dataset::open(path.to_str().unwrap()).await.unwrap();
    assert_eq!(dataset.count_rows(None).await.unwrap(), 0);
    assert_eq!(
        dataset.scan().try_into_batch().await.unwrap().schema(),
        schema
    );
}

#[tokio::test]
async fn abort_removes_dataset_and_preserves_sibling_files() {
    let temp = tempfile::tempdir().unwrap();
    let sibling = temp.path().join("keep.txt");
    tokio::fs::write(&sibling, "keep").await.unwrap();
    let path = temp.path().join("dataset");
    let batch = batch();
    let mut writer = LanceDatasetSink
        .open(path.clone(), batch.schema(), options())
        .await
        .unwrap();
    writer.write(batch).await.unwrap();
    writer.abort().await.unwrap();
    assert!(!tokio::fs::try_exists(&path).await.unwrap());
    assert_eq!(tokio::fs::read_to_string(sibling).await.unwrap(), "keep");
}

#[rstest::rstest]
#[case::default_size(None, false)]
#[case::small_target(Some(1), true)]
#[tokio::test]
async fn target_file_size_rolls_files_without_losing_rows(
    #[case] target_file_size: Option<usize>,
    #[case] should_roll: bool,
) {
    // Exceed the SDK's default page buffer while staying below its row-count limit.
    let count = 10 * 1024;
    let payloads = arrow_array::FixedSizeBinaryArray::try_from_iter((0..count).map(|i| {
        let mut state = i as u64 + 1;
        let mut bytes = [0; 1024];
        for byte in &mut bytes {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            *byte = state as u8;
        }
        bytes
    }))
    .unwrap();
    let expected = RecordBatch::try_from_iter([
        (
            "id",
            Arc::new(Int64Array::from_iter_values(0..count as i64)) as ArrayRef,
        ),
        ("payload", Arc::new(payloads) as ArrayRef),
    ])
    .unwrap();
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("dataset");
    let mut writer = LanceDatasetSink
        .open(
            path.clone(),
            expected.schema(),
            WriteOptions {
                target_file_size,
                ..options()
            },
        )
        .await
        .unwrap();
    writer.write(expected.clone()).await.unwrap();
    writer.write(expected.clone()).await.unwrap();
    assert_eq!(
        writer.commit().await.unwrap().rows_written,
        2 * count as u64
    );
    let dataset = Dataset::open(path.to_str().unwrap()).await.unwrap();
    let fragments = dataset.get_fragments();
    if should_roll {
        more_asserts::assert_gt!(fragments.len(), 1);
    } else {
        assert_eq!(fragments.len(), 1);
    }
    let actual = dataset.scan().try_into_batch().await.unwrap();
    assert_eq!(actual.schema(), expected.schema());
    let ids = actual
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let payloads = actual
        .column(1)
        .as_any()
        .downcast_ref::<arrow_array::FixedSizeBinaryArray>()
        .unwrap();
    let expected_payloads = expected
        .column(1)
        .as_any()
        .downcast_ref::<arrow_array::FixedSizeBinaryArray>()
        .unwrap();
    let mut occurrences = vec![0; count];
    for i in 0..actual.num_rows() {
        let id = usize::try_from(ids.value(i)).unwrap();
        assert_eq!(payloads.value(i), expected_payloads.value(id));
        occurrences[id] += 1;
    }
    assert_eq!(occurrences, vec![2; count]);
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
async fn downloaded_url_is_stored_as_native_lance_blob() {
    use arrow_array::{LargeBinaryArray, StringArray};
    use lance::datatypes::BlobHandling;
    use lance::index::DatasetIndexExt;
    use open_arrow_sink_core::{IndexSpec, IndexType, SinkExt};
    use open_arrow_sink_layer::DownloadBlobLayer;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut request = [0; 4096];
        let received = socket.read(&mut request).await.unwrap();
        assert!(received > 0);
        socket
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 4\r\nConnection: close\r\n\r\nblob")
            .await
            .unwrap();
    });
    let batch = RecordBatch::try_from_iter([
        ("id", Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef),
        (
            "image",
            Arc::new(StringArray::from(vec![Some(url), None])) as ArrayRef,
        ),
    ])
    .unwrap();
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("download.lance");
    let mut writer = LanceDatasetSink
        .layer(DownloadBlobLayer::new(
            ["image"].map(str::to_owned).into_iter().collect(),
        ))
        .open(
            path.clone(),
            batch.schema(),
            WriteOptions {
                indexes: vec![IndexSpec::new("id", IndexType::Scalar)],
                ..options()
            },
        )
        .await
        .unwrap();
    writer.write(batch).await.unwrap();
    assert_eq!(writer.commit().await.unwrap().rows_written, 2);
    server.await.unwrap();
    let dataset = Dataset::open(path.to_str().unwrap()).await.unwrap();
    let indexes = dataset.load_indices().await.unwrap();
    assert_eq!(indexes.len(), 1);
    assert_eq!(
        indexes[0].fields,
        vec![dataset.schema().field("id").unwrap().id]
    );
    let native_schema = arrow_schema::Schema::from(dataset.schema());
    assert_eq!(
        native_schema.field_with_name("image").unwrap().metadata()["ARROW:extension:name"],
        "lance.blob.v2"
    );
    let actual = dataset
        .scan()
        .blob_handling(BlobHandling::AllBinary)
        .try_into_batch()
        .await
        .unwrap();
    let values = actual
        .column_by_name("image")
        .unwrap()
        .as_any()
        .downcast_ref::<LargeBinaryArray>()
        .unwrap();
    assert_eq!(values.value(0), b"blob");
    assert!(values.is_null(1));
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
