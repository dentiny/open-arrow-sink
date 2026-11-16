use std::sync::Arc;

use arrow_array::{Array, ArrayRef, Int32Array, RecordBatch, StringArray, cast::AsArray};
use lance::datatypes::BlobHandling;
use open_arrow_sink_core::{Error, Result, Sink, SinkExt, async_trait};
use open_arrow_sink_lance::LanceDatasetSink;
use open_arrow_sink_layer::{DownloadBlobLayer, DownloadFailurePolicy};
use open_arrow_sink_storage::{
    BlobDownloader, DownloadContext, DownloadStream, Downloader, ObjectPath,
};
use open_arrow_sink_test_support::{memory_storage, unordered_options};

struct Source;
#[async_trait]
impl Downloader for Source {
    async fn open(&self, source: &str, _: DownloadContext) -> Result<DownloadStream> {
        let failure = || Error::NotFound("source failed".into());
        match source {
            "bad" => Err(failure()),
            "partial" => Ok(Box::pin(futures::stream::iter([
                Ok("xx".into()),
                Err(failure()),
            ]))),
            _ => Ok(Box::pin(futures::stream::iter([Ok("ok".into())]))),
        }
    }
}

fn downloader(limit: usize) -> Arc<BlobDownloader> {
    Arc::new(
        BlobDownloader::new(2.try_into().unwrap(), 128.try_into().unwrap())
            .unwrap()
            .with_download_limits(limit.try_into().unwrap(), 2.try_into().unwrap())
            .unwrap()
            .with_downloader(Arc::new(Source)),
    )
}

#[rstest::rstest]
#[case::skip(DownloadFailurePolicy::SkipRow)]
#[case::null(DownloadFailurePolicy::Null)]
#[tokio::test]
async fn source_failures_recover_with_bounded_backend_downloads(
    #[case] policy: DownloadFailurePolicy,
) {
    let input = RecordBatch::try_from_iter([
        (
            "left",
            Arc::new(StringArray::from(vec!["good", "bad", "partial"])) as ArrayRef,
        ),
        (
            "right",
            Arc::new(StringArray::from(vec!["good", "bad", "good"])) as ArrayRef,
        ),
        (
            "id",
            Arc::new(Int32Array::from(vec![10, 11, 12])) as ArrayRef,
        ),
    ])
    .unwrap();
    let storage = memory_storage("memory:///warehouse/");
    let url = storage
        .url(&ObjectPath::parse("recovered/").unwrap())
        .unwrap();
    let downloader = downloader(4);
    let backend = LanceDatasetSink::new(storage).with_blob_downloader(downloader.clone());
    let layer =
        DownloadBlobLayer::new(["left".into(), "right".into()].into()).with_failure_policy(policy);
    let metrics = layer.metrics();
    let sink = backend.clone().layer(layer);
    let mut writer = sink
        .open(url.clone(), input.schema(), unordered_options())
        .await
        .unwrap();
    writer.write(input.clone()).await.unwrap();
    writer.write(input.slice(0, 1)).await.unwrap();
    let expected = if policy == DownloadFailurePolicy::SkipRow {
        2
    } else {
        4
    };
    writer.commit().await.unwrap();
    assert_eq!(downloader.buffered_bytes(), 0);
    let observed = metrics.snapshot();
    assert_eq!(observed.blobs_failed, 3);
    assert_eq!(observed.rows_failed_blobs, 2);
    assert_eq!(
        observed.rows_skipped,
        if policy == DownloadFailurePolicy::SkipRow {
            2
        } else {
            0
        }
    );
    let actual = backend
        .dataset_builder(url)
        .unwrap()
        .load()
        .await
        .unwrap()
        .scan()
        .blob_handling(BlobHandling::AllBinary)
        .try_into_batch()
        .await
        .unwrap();
    assert_eq!(actual.num_rows(), expected as usize);
    let ids = actual
        .column(2)
        .as_primitive::<arrow_array::types::Int32Type>();
    let left = actual.column(0).as_binary::<i64>();
    let right = actual.column(1).as_binary::<i64>();
    for row in 0..actual.num_rows() {
        match ids.value(row) {
            10 => {
                assert_eq!(left.value(row), b"ok");
                assert_eq!(right.value(row), b"ok");
            }
            11 => {
                assert!(left.is_null(row));
                assert!(right.is_null(row));
            }
            12 => {
                assert!(left.is_null(row));
                assert_eq!(right.value(row), b"ok");
            }
            _ => unreachable!(),
        }
    }
}

#[rstest::rstest]
#[case::source(DownloadFailurePolicy::Fail, 4, "bad")]
#[case::budget(DownloadFailurePolicy::Null, 1, "good")]
#[tokio::test]
async fn terminal_errors_remain_terminal(
    #[case] policy: DownloadFailurePolicy,
    #[case] limit: usize,
    #[case] source: &str,
) {
    let input = RecordBatch::try_from_iter([(
        "url",
        Arc::new(StringArray::from(vec![source])) as ArrayRef,
    )])
    .unwrap();
    let storage = memory_storage("memory:///warehouse/");
    let url = storage.url(&ObjectPath::parse("failed/").unwrap()).unwrap();
    let layer = DownloadBlobLayer::new(["url".into()].into()).with_failure_policy(policy);
    let metrics = layer.metrics();
    let downloader = downloader(limit);
    let sink = LanceDatasetSink::new(storage)
        .with_blob_downloader(downloader.clone())
        .layer(layer);
    let mut writer = sink
        .open(url, input.schema(), unordered_options())
        .await
        .unwrap();
    assert!(writer.write(input.clone()).await.is_err());
    assert!(writer.write(input).await.is_err());
    assert!(writer.commit().await.is_err());
    assert_eq!(metrics.snapshot().blobs_failed, u64::from(source == "bad"));
    assert_eq!(downloader.buffered_bytes(), 0);
}
