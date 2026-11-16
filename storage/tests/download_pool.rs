#![cfg(feature = "http")]

use std::{sync::Arc, time::Duration};

use futures::{TryStreamExt, poll, stream};
use http::{Request, Response, StatusCode};
use more_asserts::assert_le;
use open_arrow_sink_storage::{DownloadPool, HttpConcurrencyLimitLayer, opendal};
use opendal::{Buffer, HttpBody, HttpTransport, HttpTransporter};
use tokio::time::timeout;

const URL: &str = "https://download-fixture.invalid";
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

fn pool(concurrency: usize, bytes: usize) -> DownloadPool {
    DownloadPool::new(concurrency.try_into().unwrap(), bytes.try_into().unwrap())
        .unwrap()
        .with_http_transport(HttpTransporter::new(Transport))
}

// Consume chunks promptly so a payload larger than the budget can make progress.
async fn read_body(pool: &DownloadPool, source: &str) -> Vec<u8> {
    timeout(Duration::from_secs(5), async {
        let mut download = pool.stream(source).await.unwrap();
        let mut body = Vec::new();
        while let Some(chunk) = download.try_next().await.unwrap() {
            assert_le!(chunk.as_ref().len(), pool.max_buffered_bytes());
            body.extend_from_slice(chunk.as_ref());
        }
        assert_eq!(pool.buffered_bytes(), 0);
        body
    })
    .await
    .expect("download stalled")
}

#[tokio::test]
async fn independent_budgets_and_cleanup() {
    timeout(Duration::from_secs(5), async {
        let first = pool(1, 8);
        let other = pool(2, 16);
        assert_eq!(first.concurrency(), 1);
        assert_eq!(other.concurrency(), 2);

        let mut held = first.stream(URL).await.unwrap();
        assert!(poll!(std::pin::pin!(first.stream(URL))).is_pending());
        assert_eq!(read_body(&other, URL).await, BODY);

        let chunk = held.try_next().await.unwrap().unwrap();
        drop(held);
        // Stream slots are released on cancellation; chunks retain byte reservations.
        let mut resumed = first.stream(URL).await.unwrap();
        assert!(poll!(resumed.try_next()).is_pending());
        assert_eq!(first.buffered_bytes(), 8);
        assert_eq!(read_body(&other, URL).await, BODY);
        drop(chunk);
        assert!(resumed.try_next().await.unwrap().is_some());
        drop(resumed);
        assert_eq!(first.buffered_bytes(), 0);
        assert_eq!(read_body(&first, URL).await, BODY);
    })
    .await
    .expect("pool budgets stalled");
}

#[tokio::test]
async fn invalid_sources_release_budgets() {
    let pool = pool(1, 8);
    for source in ["not a URL", "unsupported:///tmp/blob"] {
        assert!(pool.stream(source).await.is_err());
    }
    let mut missing = pool.stream(&format!("{URL}/missing")).await.unwrap();
    assert!(missing.try_next().await.is_err());
    drop(missing);
    assert_eq!(pool.buffered_bytes(), 0);
    assert_eq!(read_body(&pool, URL).await, BODY);
}

#[tokio::test]
async fn replacing_transport_preserves_retry_policy() {
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
    assert_eq!(read_body(&retrying, URL).await, BODY);
    assert_eq!(attempts.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn filesystem_source_streams_within_budget() {
    let pool = pool(1, 8);
    let temp = tempfile::tempdir().unwrap();
    let file = temp.path().join("source.bin");
    tokio::fs::write(&file, BODY).await.unwrap();
    let uri = open_arrow_sink_storage::Url::from_file_path(file).unwrap();
    assert_eq!(read_body(&pool, uri.as_str()).await, BODY);
}

#[tokio::test]
async fn explicit_http_budget_is_shared_across_independent_pools() {
    struct HeldBody;
    impl HttpTransport for HeldBody {
        async fn fetch(&self, _: Request<Buffer>) -> opendal::Result<Response<HttpBody>> {
            use futures::StreamExt;
            Ok(Response::builder()
                .status(StatusCode::OK)
                .header("content-length", BODY.len() + 1)
                .body(HttpBody::new(
                    stream::iter([Ok(Buffer::from(BODY))]).chain(stream::pending()),
                    Some(BODY.len() as u64 + 1),
                ))
                .unwrap())
        }
    }
    timeout(Duration::from_secs(5), async {
        let shared = HttpConcurrencyLimitLayer::new(1).unwrap();
        let make_pool = |concurrency| {
            DownloadPool::new(concurrency, 8.try_into().unwrap())
                .unwrap()
                .with_layer(shared.clone())
                .with_http_transport(HttpTransporter::new(HeldBody))
        };
        let first = make_pool(2.try_into().unwrap());
        let second = make_pool(3.try_into().unwrap());
        assert_eq!(first.concurrency(), 2);
        assert_eq!(second.concurrency(), 3);
        let mut held = first.stream(URL).await.unwrap();
        let chunk = held.try_next().await.unwrap().unwrap();
        let mut waiting = second.stream(URL).await.unwrap();
        assert!(poll!(waiting.try_next()).is_pending());
        // The retained chunk owns only its pool's byte reservation. Dropping the
        // first HTTP body releases the shared request slot for the second pool.
        drop(held);
        assert!(waiting.try_next().await.unwrap().is_some());
        assert_eq!(first.buffered_bytes(), 8);
        drop(chunk);
        assert_eq!(first.buffered_bytes(), 0);
    })
    .await
    .expect("shared HTTP budget stalled");
}

#[test]
fn rejects_concurrency_above_semaphore_capacity() {
    assert!(
        DownloadPool::new(
            (tokio::sync::Semaphore::MAX_PERMITS + 1)
                .try_into()
                .unwrap(),
            8.try_into().unwrap(),
        )
        .is_err()
    );
}

#[derive(Clone)]
struct PolicyTransport {
    attempts: Arc<std::sync::atomic::AtomicUsize>,
}
impl HttpTransport for PolicyTransport {
    async fn fetch(&self, request: Request<Buffer>) -> opendal::Result<Response<HttpBody>> {
        use std::sync::atomic::Ordering;
        let attempt = self.attempts.fetch_add(1, Ordering::SeqCst);
        let path = request.uri().path();
        if path == "/headers-stall" {
            return std::future::pending().await;
        }
        if path == "/body-stall" {
            return Ok(Response::builder()
                .header("content-length", 11)
                .body(HttpBody::new(stream::pending(), Some(11)))
                .unwrap());
        }
        if matches!(path, "/status-429" | "/status-503") && attempt == 0 {
            return Ok(Response::builder()
                .status(if path == "/status-429" { 429 } else { 503 })
                .body(HttpBody::new(stream::empty(), Some(0)))
                .unwrap());
        }
        if path.starts_with("/resume") && attempt == 0 {
            return Ok(Response::builder()
                .header("content-length", 11)
                .header("etag", "\"v1\"")
                .body(HttpBody::new(
                    stream::iter([
                        Ok(Buffer::from(b"hello ".as_slice())),
                        Err(
                            opendal::Error::new(opendal::ErrorKind::Unexpected, "interrupted")
                                .set_temporary(),
                        ),
                    ]),
                    Some(11),
                ))
                .unwrap());
        }
        if path.starts_with("/resume") {
            assert_eq!(request.headers()["range"], "bytes=6-");
            assert_eq!(request.headers()["if-range"], "\"v1\"");
            return Ok(Response::builder()
                .status(if path == "/resume-ignore-range" {
                    200
                } else {
                    206
                })
                .header("content-length", 5)
                .header(
                    "content-range",
                    if path == "/resume-wrong-range" {
                        "bytes 0-4/11"
                    } else {
                        "bytes 6-10/11"
                    },
                )
                .header(
                    "etag",
                    if path == "/resume-changed" {
                        "\"v2\""
                    } else {
                        "\"v1\""
                    },
                )
                .body(HttpBody::new(
                    stream::iter([Ok(Buffer::from(b"world".as_slice()))]),
                    Some(5),
                ))
                .unwrap());
        }
        Ok(Response::builder()
            .header("content-length", 11)
            .body(HttpBody::new(
                stream::iter([Ok(Buffer::from(b"hello world".as_slice()))]),
                Some(11),
            ))
            .unwrap())
    }
}

#[tokio::test]
async fn download_policies_bound_stalls_and_resume_without_duplicating_bytes() {
    use open_arrow_sink_storage::{RetryConfig, StorageConfig, TimeoutConfig};
    use std::sync::atomic::{AtomicUsize, Ordering};
    for path in [
        "/status-429",
        "/status-503",
        "/headers-stall",
        "/body-stall",
        "/resume",
        "/resume-ignore-range",
        "/resume-wrong-range",
        "/resume-changed",
    ] {
        let attempts = Arc::new(AtomicUsize::new(0));
        let pool = DownloadPool::new_with_config(
            4.try_into().unwrap(),
            16.try_into().unwrap(),
            StorageConfig {
                timeout: Some(TimeoutConfig {
                    operation_timeout: Duration::from_millis(20),
                    io_timeout: Duration::from_millis(20),
                }),
                retry: Some(RetryConfig {
                    max_retries: 1,
                    min_delay: Duration::from_millis(1),
                    max_delay: Duration::from_millis(1),
                    jitter: false,
                    ..RetryConfig::default()
                }),
            },
        )
        .unwrap()
        .with_http_transport(HttpTransporter::new(PolicyTransport {
            attempts: attempts.clone(),
        }));
        let result = timeout(Duration::from_secs(2), async {
            let mut stream = pool
                .stream(&format!("https://policy-fixture.invalid{path}"))
                .await?;
            let mut bytes = Vec::new();
            while let Some(chunk) = stream.try_next().await? {
                bytes.extend_from_slice(chunk.as_ref());
            }
            Ok::<_, open_arrow_sink_core::Error>(bytes)
        })
        .await
        .expect("configured download deadline did not bound stalled I/O");
        if matches!(path, "/status-429" | "/status-503" | "/resume") {
            assert_eq!(result.unwrap(), b"hello world", "{path}");
            assert_eq!(attempts.load(Ordering::SeqCst), 2, "{path}");
        } else {
            assert!(
                result.is_err(),
                "{path} must fail rather than return corrupt bytes"
            );
            assert_eq!(attempts.load(Ordering::SeqCst), 2, "{path}");
        }
        assert_eq!(pool.buffered_bytes(), 0);
    }
}

#[tokio::test]
async fn default_http_policy_retries_a_transient_status() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    let attempts = Arc::new(AtomicUsize::new(0));
    let pool = DownloadPool::new(4.try_into().unwrap(), 16.try_into().unwrap())
        .unwrap()
        .with_http_transport(HttpTransporter::new(PolicyTransport {
            attempts: attempts.clone(),
        }));
    timeout(Duration::from_secs(4), async {
        let mut stream = pool
            .stream("https://policy-fixture.invalid/status-503")
            .await
            .unwrap();
        let mut bytes = Vec::new();
        while let Some(chunk) = stream.try_next().await.unwrap() {
            bytes.extend_from_slice(chunk.as_ref());
        }
        assert_eq!(bytes, b"hello world");
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
    })
    .await
    .unwrap();
}
