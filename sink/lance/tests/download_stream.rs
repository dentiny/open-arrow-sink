use arrow_array::cast::AsArray;
use arrow_array::{Array, ArrayRef, Int32Array, LargeBinaryArray, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use lance::{Dataset, datatypes::BlobHandling};
use open_arrow_sink_core::{BLOB_METADATA_KEY, BlobHandleConfig, IoMetrics, Sink, SinkExt};
use open_arrow_sink_lance::{LanceDatasetSink, Url};
use open_arrow_sink_layer::{DownloadBlobLayer, UnorderedWriteLayer};
use open_arrow_sink_storage::BlobDownloader;
use open_arrow_sink_test_support::unordered_options as options;
use std::{num::NonZeroUsize, sync::Arc};
use testutil::server;

mod testutil;

#[tokio::test]
async fn downloaded_payloads_use_sdk_layout_and_round_trip() {
    let server = server().await;
    let downloader = BlobDownloader::new(
        NonZeroUsize::new(2).unwrap(),
        NonZeroUsize::new(128 * 1024 * 1024).unwrap(),
    )
    .unwrap()
    .with_download_limits(
        NonZeroUsize::new(40 * 1024 * 1024).unwrap(),
        NonZeroUsize::new(2 * 1024 * 1024).unwrap(),
    )
    .unwrap();
    let io = IoMetrics::default();
    let large = format!("{}/large", server.url);
    let empty = format!("{}/empty", server.url);
    let values = StringArray::from(vec![
        Some(large.as_str()),
        None,
        Some(large.as_str()),
        Some(empty.as_str()),
    ]);
    let schema = Arc::new(Schema::new(vec![
        Field::new("blob", DataType::Utf8, true),
        Field::new("id", DataType::Int32, false),
        Field::new("bytes", DataType::LargeBinary, true)
            .with_metadata([(BLOB_METADATA_KEY.into(), "true".into())].into()),
    ]));
    let input = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(values),
            Arc::new(Int32Array::from(vec![0, 1, 2, 3])) as ArrayRef,
            Arc::new(LargeBinaryArray::from(vec![
                Some(b"mem".as_slice()),
                None,
                Some(b"mem2".as_slice()),
                Some(b"".as_slice()),
            ])) as ArrayRef,
        ],
    )
    .unwrap();
    let downloader = Arc::new(downloader);
    let sink = LanceDatasetSink::default()
        .with_blob_downloader(downloader.clone())
        .with_encoding_buffer_size(NonZeroUsize::new(4 * 1024 * 1024).unwrap());
    let temp = tempfile::tempdir().unwrap();
    // URL-looking text stays text without an explicit download layer.
    let plain_path = temp.path().join("plain");
    let mut plain = sink
        .open(
            Url::from_directory_path(&plain_path).unwrap(),
            schema.clone(),
            options(),
        )
        .await
        .unwrap();
    plain.write(input.clone()).await.unwrap();
    plain.commit().await.unwrap();
    let plain = Dataset::open(plain_path.to_str().unwrap())
        .await
        .unwrap()
        .scan()
        .try_into_batch()
        .await
        .unwrap();
    assert_eq!(plain.schema().field(0).data_type(), &DataType::Utf8);
    assert_eq!(
        plain
            .column(0)
            .as_string::<i32>()
            .iter()
            .flatten()
            .filter(|url| *url == large.as_str())
            .count(),
        2
    );
    let sink = sink
        .layer(DownloadBlobLayer::new(["blob".to_owned()].into()))
        .layer(UnorderedWriteLayer::new(NonZeroUsize::new(2).unwrap()));
    for selected in ["missing", "id"] {
        let invalid =
            LanceDatasetSink::default().layer(DownloadBlobLayer::new([selected.to_owned()].into()));
        assert!(
            invalid
                .open(
                    Url::from_directory_path(temp.path().join("invalid")).unwrap(),
                    schema.clone(),
                    options()
                )
                .await
                .is_err()
        );
    }
    assert!(!temp.path().join("invalid").exists());
    let path = temp.path().join("dataset");
    let mut writer = sink
        .open(
            Url::from_directory_path(&path).unwrap(),
            schema.clone(),
            options()
                .with_io_metrics(io.clone())
                .with_blob_handle(BlobHandleConfig {
                    inline_size_threshold: Some(16),
                    dedicated_size_threshold: NonZeroUsize::new(32 * 1024 * 1024),
                    ..Default::default()
                }),
        )
        .await
        .unwrap();
    assert_eq!(writer.schema(), &schema);
    writer.write(input).await.unwrap();
    writer.commit().await.unwrap();
    assert_eq!(downloader.buffered_bytes(), 0);
    assert_eq!(io.snapshot().bytes_read, 34 * 1024 * 1024);
    assert!(io.snapshot().bytes_written > 34 * 1024 * 1024);
    drop(server);
    let dataset = Arc::new(Dataset::open(path.to_str().unwrap()).await.unwrap());
    let descriptors = dataset.scan().try_into_batch().await.unwrap();
    let ids = descriptors
        .column(1)
        .as_primitive::<arrow_array::types::Int32Type>();
    let first = (0..ids.len()).find(|&row| ids.value(row) == 0).unwrap();
    let second = (0..ids.len()).find(|&row| ids.value(row) == 2).unwrap();
    let packed = dataset
        .take_blobs_by_indices(&[first as u64, second as u64], "blob")
        .await
        .unwrap();
    let first = packed[0].as_ref().unwrap();
    let second = packed[1].as_ref().unwrap();
    assert_eq!(first.data_path(), second.data_path());
    assert_eq!(second.position() - first.position(), 17 * 1024 * 1024);
    let actual = dataset
        .scan()
        .blob_handling(BlobHandling::AllBinary)
        .try_into_batch()
        .await
        .unwrap();
    let values = actual.column(0).as_binary::<i64>();
    let memory = actual.column(2).as_binary::<i64>();
    let ids = actual
        .column(1)
        .as_primitive::<arrow_array::types::Int32Type>();
    for row in 0..actual.num_rows() {
        match ids.value(row) {
            0 => assert_eq!(memory.value(row), b"mem"),
            1 => assert!(memory.is_null(row)),
            2 => assert_eq!(memory.value(row), b"mem2"),
            3 => assert_eq!(memory.value(row), b""),
            _ => unreachable!(),
        }
        match ids.value(row) {
            0 | 2 => {
                assert_eq!(values.value(row).len(), 17 * 1024 * 1024);
                assert!(values.value(row).iter().all(|&byte| byte == b'x'));
            }
            1 => assert!(values.is_null(row)),
            3 => {
                assert!(!values.is_null(row));
                assert!(values.value(row).is_empty());
            }
            _ => unreachable!(),
        }
    }
}
