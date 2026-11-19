use open_arrow_sink_test_support::url as fixture_url;
use std::sync::Arc;

use arrow_array::{Array, ArrayRef, Int32Array, RecordBatch, StringArray, cast::AsArray};
use lance::datatypes::BlobHandling;
use open_arrow_sink_core::{Error, Result, Sink, SinkExt, async_trait};
use open_arrow_sink_lance::LanceDatasetSink;
use open_arrow_sink_layer::{
    DownloadBlobLayer, DownloadFailurePolicy, MetricsLayer, ParallelIngestionLayer,
};
use open_arrow_sink_storage::{BlobDownloader, DownloadContext, DownloadStream, Downloader};
use open_arrow_sink_test_support::{memory_warehouse, unordered_options};

struct Source;
#[async_trait]
impl Downloader for Source {
    async fn open(&self, source: &str, _: DownloadContext) -> Result<DownloadStream> {
        let failure = || Error::NotFound("source failed".into());
        match source {
            "bad" => Err(failure()),
            "config" => Err(Error::InvalidConfig("bad credentials".into())),
            "unknown" => Err(Error::backend(std::io::Error::other("source unavailable"))),
            "partial_config" => Ok(Box::pin(futures::stream::iter([
                Ok("xx".into()),
                Err(Error::InvalidConfig("bad credentials".into())),
            ]))),
            "partial" => Ok(Box::pin(futures::stream::iter([
                Ok("xx".into()),
                Err(failure()),
            ]))),
            _ => Ok(Box::pin(futures::stream::iter([Ok("ok".into())]))),
        }
    }
}

fn downloader() -> Arc<BlobDownloader> {
    Arc::new(
        BlobDownloader::new(2.try_into().unwrap())
            .unwrap()
            .with_downloader(Arc::new(Source)),
    )
}

#[rstest::rstest]
#[case::skip(DownloadFailurePolicy::SkipRow, false)]
#[case::null(DownloadFailurePolicy::Null, false)]
#[case::parallel_skip(DownloadFailurePolicy::SkipRow, true)]
#[case::parallel_null(DownloadFailurePolicy::Null, true)]
#[tokio::test]
async fn source_failures_recover_with_bounded_backend_downloads(
    #[case] policy: DownloadFailurePolicy,
    #[case] parallel: bool,
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
    let storage = memory_warehouse();
    let url = fixture_url(&storage, "recovered/");
    let downloader = downloader();
    let backend = LanceDatasetSink::new(storage).with_blob_downloader(downloader.clone());
    let layer =
        DownloadBlobLayer::new(["left".into(), "right".into()].into()).with_failure_policy(policy);
    let metrics = layer.metrics();
    let write_metrics = MetricsLayer::default();
    let written = write_metrics.metrics();
    let sink = backend.clone().layer(layer).layer(write_metrics);
    // Exercise both the single-writer adapter and unordered worker aggregation.
    let sink: Box<dyn Sink<Request = open_arrow_sink_lance::Url>> = if parallel {
        Box::new(sink.layer(ParallelIngestionLayer::fixed(2.try_into().unwrap())))
    } else {
        Box::new(sink)
    };
    let mut writer = sink
        .open(url.clone(), input.schema(), unordered_options())
        .await
        .unwrap();
    // Overlapping sessions share cumulative metrics but must return independent summaries.
    let other_url = open_arrow_sink_lance::Url::parse("memory:///warehouse/other/").unwrap();
    let mut other = sink
        .open(other_url, input.schema(), unordered_options())
        .await
        .unwrap();
    other.write(input.slice(0, 1)).await.unwrap();
    writer.write(input.clone()).await.unwrap();
    writer.write(input.slice(0, 1)).await.unwrap();
    let expected = if policy == DownloadFailurePolicy::SkipRow {
        2
    } else {
        4
    };
    let summary = writer.commit().await.unwrap();
    assert_eq!(summary.rows_committed, expected);
    assert_eq!(summary.rows_failed_blobs, 2);
    assert_eq!(
        summary.rows_skipped,
        if policy == DownloadFailurePolicy::SkipRow {
            2
        } else {
            0
        }
    );

    let other_summary = other.commit().await.unwrap();
    assert_eq!(other_summary.rows_committed, 1);
    assert_eq!(other_summary.rows_failed_blobs, 0);
    assert_eq!(other_summary.rows_skipped, 0);

    assert_eq!(written.snapshot().rows_committed, expected + 1);
    // A later empty append returns only this session's counts.
    let mut append = unordered_options();
    append.mode = open_arrow_sink_core::WriteMode::Append;
    let empty = sink
        .open(url.clone(), input.schema(), append)
        .await
        .unwrap()
        .commit()
        .await
        .unwrap();
    assert_eq!(empty.rows_committed, 0);
    assert_eq!(empty.rows_failed_blobs, 0);
    assert_eq!(empty.rows_skipped, 0);

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

#[tokio::test]
async fn write_all_returns_actual_blob_recovery_counts() {
    let input = RecordBatch::try_from_iter([(
        "url",
        Arc::new(StringArray::from(vec!["good", "bad", "partial"])) as ArrayRef,
    )])
    .unwrap();
    let storage = memory_warehouse();
    let url = fixture_url(&storage, "write-all/");
    let sink = LanceDatasetSink::new(storage)
        .with_blob_downloader(downloader())
        .layer(
            DownloadBlobLayer::new(["url".into()].into())
                .with_failure_policy(DownloadFailurePolicy::SkipRow),
        )
        .layer(ParallelIngestionLayer::fixed(2.try_into().unwrap()));
    let summary = sink
        .write_all(
            url,
            input.schema(),
            unordered_options(),
            futures::stream::iter([Ok::<_, open_arrow_sink_core::Error>(input)]),
        )
        .await
        .unwrap();
    assert_eq!(summary.rows_committed, 1);
    assert_eq!(summary.rows_failed_blobs, 2);
    assert_eq!(summary.rows_skipped, 2);
}

#[rstest::rstest]
#[case(DownloadFailurePolicy::SkipRow)]
#[case(DownloadFailurePolicy::Null)]
#[tokio::test]
async fn recovery_propagates_fatal_errors(#[case] policy: DownloadFailurePolicy) {
    use open_arrow_sink_storage::SourceDownloadError;
    let layer = DownloadBlobLayer::new(["url".into()].into()).with_failure_policy(policy);
    let metrics = layer.metrics();
    let sink = LanceDatasetSink::new(memory_warehouse())
        .with_blob_downloader(downloader())
        .layer(layer)
        .layer(ParallelIngestionLayer::fixed(2.try_into().unwrap()));
    for source in ["config", "partial_config", "unknown"] {
        let input = RecordBatch::try_from_iter([(
            "url",
            Arc::new(StringArray::from(vec![source])) as ArrayRef,
        )])
        .unwrap();
        let error = sink
            .write_all(
                format!("memory:///warehouse/{source}/").parse().unwrap(),
                input.schema(),
                unordered_options(),
                futures::stream::iter([Ok(input)]),
            )
            .await
            .unwrap_err();
        assert!(!SourceDownloadError::find(&error).unwrap().recoverable);
    }
    assert_eq!(metrics.snapshot().rows_skipped, 0);
}
