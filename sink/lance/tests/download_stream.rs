use std::{num::NonZeroUsize, sync::Arc};

use arrow_array::{Array, ArrayRef, Int32Array, LargeBinaryArray, RecordBatch, StringArray};
use lance::{Dataset, datatypes::BlobHandling};
use open_arrow_sink_core::{Ordering, Sink, SinkExt, WriteOptions};
use open_arrow_sink_lance::{LanceDatasetSink, Url};
use open_arrow_sink_layer::{DownloadBlobLayer, DownloadPool};
mod testutil;
use testutil::server;

fn options() -> WriteOptions {
    WriteOptions {
        ordering: Ordering::Unordered,
        ..Default::default()
    }
}
fn pool() -> Arc<DownloadPool> {
    Arc::new(
        DownloadPool::new(
            NonZeroUsize::new(2).unwrap(),
            NonZeroUsize::new(1024).unwrap(),
        )
        .unwrap(),
    )
}

#[tokio::test]
async fn downloads_larger_than_budget_round_trip_as_owned_native_blobs() {
    let server = server().await;
    let large = format!("{}/large", server.url);
    let empty = format!("{}/empty", server.url);
    let input = RecordBatch::try_from_iter([
        (
            "url",
            Arc::new(StringArray::from(vec![Some(large), None, Some(empty)])) as ArrayRef,
        ),
        ("id", Arc::new(Int32Array::from(vec![0, 1, 2])) as ArrayRef),
    ])
    .unwrap();
    let budget = pool();
    let inner_metrics_layer = open_arrow_sink_layer::MetricsLayer::default();
    let inner_metrics = inner_metrics_layer.metrics();
    let outer_metrics_layer = open_arrow_sink_layer::MetricsLayer::default();
    let outer_metrics = outer_metrics_layer.metrics();
    let sink = LanceDatasetSink::default()
        .layer(inner_metrics_layer)
        .layer(
            DownloadBlobLayer::new(["url".to_owned()].into_iter().collect())
                .with_download_pool(budget.clone()),
        )
        .layer(outer_metrics_layer);
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("dataset");
    let mut writer = sink
        .open(
            Url::from_directory_path(&path).unwrap(),
            input.schema(),
            options(),
        )
        .await
        .unwrap();
    writer.write(input).await.unwrap();
    assert_eq!(budget.buffered_bytes(), 0);
    let summary = writer.commit().await.unwrap();
    assert_eq!(summary.rows_written, 3);
    assert!(summary.bytes_read.unwrap() >= 17 * 1024 * 1024);
    assert!(summary.bytes_written.unwrap() > 17 * 1024 * 1024);
    let observed = outer_metrics.snapshot();
    assert_eq!(observed.rows_committed, 3);
    assert_eq!(observed.rows_accepted, 3);
    assert_eq!(observed.bytes_read, summary.bytes_read);
    assert_eq!(observed.bytes_written, summary.bytes_written);
    assert_eq!(inner_metrics.snapshot().rows_committed, 3);
    assert_eq!(
        inner_metrics.snapshot().bytes_written,
        summary.bytes_written
    );
    drop(server);
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
    let ids = actual
        .column(1)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    for row in 0..actual.num_rows() {
        match ids.value(row) {
            0 => {
                assert_eq!(values.value(row).len(), 17 * 1024 * 1024);
                assert!(values.value(row).iter().all(|&byte| byte == b'x'));
            }
            1 => assert!(values.is_null(row)),
            2 => {
                assert!(!values.is_null(row));
                assert!(values.value(row).is_empty());
            }
            _ => unreachable!(),
        }
    }
}
