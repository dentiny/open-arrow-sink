use std::{num::NonZeroUsize, sync::Arc};

use arrow_array::{Array, ArrayRef, Int32Array, LargeBinaryArray, RecordBatch, StringArray};
use lance::{Dataset, datatypes::BlobHandling};
use open_arrow_sink_core::{Ordering, Sink, SinkExt, WriteOptions};
use open_arrow_sink_lance::{LanceDatasetSink, Url};
use open_arrow_sink_layer::{DownloadBlobLayer, DownloadFailurePolicy, DownloadPool};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

#[rstest::rstest]
#[case::skip(DownloadFailurePolicy::SkipRow, false)]
#[case::null(DownloadFailurePolicy::Null, false)]
#[case::skip_all(DownloadFailurePolicy::SkipRow, true)]
#[case::null_all(DownloadFailurePolicy::Null, true)]
#[tokio::test]
async fn failed_sources_are_discarded_without_poisoning_native_session(
    #[case] policy: DownloadFailurePolicy,
    #[case] all_failed: bool,
) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        loop {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = [0; 4096];
            let n = socket.read(&mut request).await.unwrap();
            let request = String::from_utf8_lossy(&request[..n]);
            let path = request.split_whitespace().nth(1).unwrap();
            let response = match path {
                "/missing" => {
                    b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                        .as_slice()
                }
                // A late source failure after a chunk has reached the native upload.
                "/partial" => {
                    b"HTTP/1.1 200 OK\r\nContent-Length: 4\r\nConnection: close\r\n\r\nabc"
                }
                _ => b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok",
            };
            let _ = socket.write_all(response).await;
        }
    });
    let paths = if all_failed {
        ["/missing", "/missing", "/partial", "/partial"]
    } else {
        ["/ok", "/missing", "/partial", "/ok"]
    };
    let urls = paths.map(|p| format!("{base}{p}"));
    let input = RecordBatch::try_from_iter([
        (
            "left",
            Arc::new(StringArray::from(urls.to_vec())) as ArrayRef,
        ),
        (
            "right",
            Arc::new(StringArray::from(vec![format!("{base}/ok"); 4])) as ArrayRef,
        ),
        (
            "id",
            Arc::new(Int32Array::from(vec![0, 1, 2, 3])) as ArrayRef,
        ),
    ])
    .unwrap();
    let pool = Arc::new(
        DownloadPool::new(
            NonZeroUsize::new(2).unwrap(),
            NonZeroUsize::new(1024).unwrap(),
        )
        .unwrap(),
    );
    let layer = DownloadBlobLayer::new(["left".into(), "right".into()].into())
        .with_download_pool(pool.clone())
        .with_failure_policy(policy);
    let metrics = layer.metrics();
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("dataset");
    let mut options = WriteOptions::default();
    options.ordering = Ordering::Unordered;
    let mut writer = LanceDatasetSink::default()
        .layer(layer)
        .open(
            Url::from_directory_path(&path).unwrap(),
            input.schema(),
            options,
        )
        .await
        .unwrap();
    writer.write(input).await.unwrap();
    assert_eq!(pool.buffered_bytes(), 0);
    let summary = writer.commit().await.unwrap();
    server.abort();
    let failed = if all_failed { 4 } else { 2 };
    let expected_rows = if policy == DownloadFailurePolicy::SkipRow {
        4 - failed
    } else {
        4
    };
    assert_eq!(summary.rows_written, expected_rows);
    assert_eq!(metrics.snapshot().blobs_failed, failed);
    assert_eq!(metrics.snapshot().rows_failed_blobs, failed);
    let dataset = Dataset::open(path.to_str().unwrap()).await.unwrap();
    let output = dataset
        .scan()
        .blob_handling(BlobHandling::AllBinary)
        .try_into_batch()
        .await
        .unwrap();
    let left = output
        .column(0)
        .as_any()
        .downcast_ref::<LargeBinaryArray>()
        .unwrap();
    let right = output
        .column(1)
        .as_any()
        .downcast_ref::<LargeBinaryArray>()
        .unwrap();
    let ids = output
        .column(2)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(output.num_rows() as u64, expected_rows);
    for row in 0..output.num_rows() {
        assert_eq!(right.value(row), b"ok");
        if all_failed || matches!(ids.value(row), 1 | 2) {
            assert!(left.is_null(row));
        } else {
            assert_eq!(left.value(row), b"ok");
        }
    }
    // Successful blobs for skipped rows are also removed at native commit.
    let files: usize = std::fs::read_dir(path.join("_blobs"))
        .unwrap()
        .map(|dir| std::fs::read_dir(dir.unwrap().path()).unwrap().count())
        .sum();
    assert_eq!(
        files,
        if policy == DownloadFailurePolicy::SkipRow {
            2 * (4 - failed) as usize
        } else {
            (8 - failed) as usize
        }
    );
}
