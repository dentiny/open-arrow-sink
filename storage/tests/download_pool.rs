#![cfg(feature = "http")]

use std::{sync::Arc, time::Duration};

use futures::{TryStreamExt, stream};
use http::{Request, Response, StatusCode};
use more_asserts::assert_le;
use open_arrow_sink_storage::{DownloadPool, opendal};
use opendal::{Buffer, HttpBody, HttpTransport, HttpTransporter};
use tokio::time::timeout;

const BODY: &[u8] = b"a payload larger than either pool budget";

struct Transport;
impl HttpTransport for Transport {
    async fn fetch(&self, request: Request<Buffer>) -> opendal::Result<Response<HttpBody>> {
        let missing = request.uri().path() == "/missing";
        if missing {
            // Error classification must discard even an endless diagnostic body.
            return Ok(Response::builder()
                .status(StatusCode::NOT_FOUND)
                .header("content-length", 999999999)
                .body(HttpBody::new(stream::pending(), Some(999999999)))
                .unwrap());
        }
        let body = BODY;
        Ok(Response::builder()
            .status(StatusCode::OK)
            .header("content-length", body.len())
            .body(HttpBody::new(
                stream::iter([Ok(Buffer::from(body))]),
                Some(body.len() as u64),
            ))
            .unwrap())
    }
}

fn pool(concurrency: usize, bytes: usize) -> Arc<DownloadPool> {
    Arc::new(
        DownloadPool::new(concurrency.try_into().unwrap(), bytes.try_into().unwrap())
            .unwrap()
            .with_http_transport(HttpTransporter::new(Transport)),
    )
}

// Run sequentially because all pools share the process-wide request limit.
#[tokio::test]
async fn streaming_limits_and_cleanup() {
    timeout(Duration::from_secs(5), async {
        let url = "https://download-fixture.invalid";
        let first = pool(2, 8);
        let other = pool(64, 16);
        assert_eq!(first.concurrency(), other.concurrency());

        let mut streams = Vec::new();
        for _ in 0..first.concurrency() {
            streams.push(first.stream(url).await.unwrap());
        }
        assert!(
            timeout(Duration::from_millis(20), other.stream(url))
                .await
                .is_err()
        );
        let chunk = streams[0].try_next().await.unwrap().unwrap();
        drop(streams);

        // Dropping streams releases request slots, but retained chunks keep their budget.
        let mut resumed = first.stream(url).await.unwrap();
        assert!(
            timeout(Duration::from_millis(20), resumed.try_next())
                .await
                .is_err()
        );
        drop(resumed);
        let mut independent = other.stream(url).await.unwrap();
        let other_chunk = independent.try_next().await.unwrap().unwrap();
        assert_eq!(first.buffered_bytes(), 8);
        assert_eq!(other.buffered_bytes(), 16);
        drop((chunk, other_chunk, independent));
        assert_eq!(first.buffered_bytes(), 0);
        assert_eq!(other.buffered_bytes(), 0);

        let mut download = first.stream(url).await.unwrap();
        let mut body = Vec::new();
        while let Some(chunk) = download.try_next().await.unwrap() {
            assert_le!(chunk.as_ref().len(), 8);
            body.extend_from_slice(chunk.as_ref());
        }
        drop(download);
        assert_eq!(body, BODY);
        for source in ["not a URL", "unsupported:///tmp/blob"] {
            assert!(first.stream(source).await.is_err());
        }
        let mut missing = first.stream(&format!("{url}/missing")).await.unwrap();
        assert!(missing.try_next().await.is_err());
        drop(missing);
        assert_eq!(first.buffered_bytes(), 0);
        assert!(first.stream(url).await.is_ok());

        // Replacing the transport after layering preserves retry policy.
        use std::sync::atomic::{AtomicUsize, Ordering};
        struct Transient(Arc<AtomicUsize>);
        impl HttpTransport for Transient {
            async fn fetch(&self, request: Request<Buffer>) -> opendal::Result<Response<HttpBody>> {
                let mut response = Transport.fetch(request).await?;
                if self.0.fetch_add(1, Ordering::SeqCst) == 0 {
                    *response.status_mut() = StatusCode::SERVICE_UNAVAILABLE;
                }
                Ok(response)
            }
        }
        let attempts = Arc::new(AtomicUsize::new(0));
        let retrying = DownloadPool::new(2.try_into().unwrap(), 8.try_into().unwrap())
            .unwrap()
            .with_layer(
                opendal::layers::RetryLayer::default()
                    .with_min_delay(Duration::from_millis(1))
                    .with_max_delay(Duration::from_millis(1))
                    .with_max_times(1),
            )
            .with_http_transport(HttpTransporter::new(Transient(attempts.clone())));
        let mut retried = retrying.stream(url).await.unwrap();
        let mut bytes = Vec::new();
        while let Some(chunk) = retried.try_next().await.unwrap() {
            bytes.extend_from_slice(chunk.as_ref());
        }
        assert_eq!(bytes, BODY);
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
        drop(retried);
        let temp = tempfile::tempdir().unwrap();
        let file = temp.path().join("source.bin");
        tokio::fs::write(&file, BODY).await.unwrap();
        let uri = open_arrow_sink_storage::Url::from_file_path(file).unwrap();
        let mut local = first.stream(uri.as_str()).await.unwrap();
        let mut bytes = Vec::new();
        while let Some(chunk) = local.try_next().await.unwrap() {
            assert_le!(chunk.as_ref().len(), 8);
            bytes.extend_from_slice(chunk.as_ref());
        }
        assert_eq!(bytes, BODY);
        assert_eq!(first.buffered_bytes(), 0);
    })
    .await
    .expect("download pool test stalled");
}
