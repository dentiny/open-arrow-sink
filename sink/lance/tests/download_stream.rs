use arrow_array::cast::AsArray;
use std::{num::NonZeroUsize, sync::Arc};

use arrow_array::{Array, ArrayRef, Int32Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use lance::{
    Dataset,
    blob::{BlobArrayBuilder, blob_field},
    datatypes::BlobHandling,
};
use open_arrow_sink_core::{BlobHandleConfig, IoMetrics, Sink};
use open_arrow_sink_lance::{LanceDatasetSink, Url};
use open_arrow_sink_storage::BlobDownloader;
mod testutil;
use open_arrow_sink_test_support::unordered_options as options;
use testutil::server;

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
    let mut values = BlobArrayBuilder::new(4);
    values.push_uri(&large).unwrap();
    values.push_null().unwrap();
    values.push_uri(&large).unwrap();
    values.push_uri(&empty).unwrap();
    let schema = Arc::new(Schema::new(vec![
        blob_field("blob", true),
        Field::new("id", DataType::Int32, false),
    ]));
    let input = RecordBatch::try_new(
        schema.clone(),
        vec![
            values.finish().unwrap(),
            Arc::new(Int32Array::from(vec![0, 1, 2, 3])) as ArrayRef,
        ],
    )
    .unwrap();
    let downloader = Arc::new(downloader);
    let sink = LanceDatasetSink::default()
        .with_blob_downloader(downloader.clone())
        .with_encoding_buffer_size(NonZeroUsize::new(4 * 1024 * 1024).unwrap());
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("dataset");
    let mut writer = sink
        .open(
            Url::from_directory_path(&path).unwrap(),
            schema.clone(),
            options()
                .with_io_metrics(Some(io.clone()))
                .with_blob_handle(Some(BlobHandleConfig {
                    inline_size_threshold: Some(0),
                    dedicated_size_threshold: NonZeroUsize::new(32 * 1024 * 1024),
                    ..Default::default()
                })),
        )
        .await
        .unwrap();
    writer.write(input).await.unwrap();
    assert!(downloader.buffered_bytes() <= downloader.max_buffered_bytes());
    assert_eq!(writer.commit().await.unwrap().rows_written, 4);
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
    let ids = actual
        .column(1)
        .as_primitive::<arrow_array::types::Int32Type>();
    for row in 0..actual.num_rows() {
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
