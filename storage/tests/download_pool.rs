use std::{sync::Arc, time::Duration};

use futures::{TryStreamExt, poll, stream};
use http::{Request, Response, StatusCode};
use more_asserts::assert_le;
use open_arrow_sink_storage::{
    DownloadPool, HttpConcurrencyLimitLayer, RetryConfig, StorageConfig, opendal,
};
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

fn pool_with(
    concurrency: usize,
    bytes: usize,
    config: StorageConfig,
    transport: impl HttpTransport,
) -> DownloadPool {
    let http = DownloadPool::http_storage(config)
        .unwrap()
        .with_http_transport(HttpTransporter::new(transport));
    DownloadPool::new(concurrency.try_into().unwrap(), bytes.try_into().unwrap())
        .unwrap()
        .with_http_storage(http)
        .unwrap()
}

fn pool(concurrency: usize, bytes: usize) -> DownloadPool {
    pool_with(concurrency, bytes, StorageConfig::default(), Transport)
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
async fn http_storage_applies_retry_policy_around_custom_transport() {
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
    let config = StorageConfig {
        retry: Some(RetryConfig {
            max_retries: 1,
            min_delay: Duration::from_millis(1),
            max_delay: Duration::from_millis(1),
            ..RetryConfig::default()
        }),
        ..StorageConfig::default()
    };
    let retrying = pool_with(2, 8, config, Transient(attempts.clone()));
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
            let http = DownloadPool::http_storage(StorageConfig::default())
                .unwrap()
                .with_http_concurrency_limit(shared.clone())
                .with_http_transport(HttpTransporter::new(HeldBody));
            DownloadPool::new(concurrency, 8.try_into().unwrap())
                .unwrap()
                .with_http_storage(http)
                .unwrap()
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
        let pool = pool_with(
            4,
            16,
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
            PolicyTransport {
                attempts: attempts.clone(),
            },
        );
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
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let source = format!("http://{}/status-503", listener.local_addr().unwrap());
    let requests = attempts.clone();
    let server = tokio::spawn(async move {
        for attempt in 0..2 {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = [0; 4096];
            let count = socket.read(&mut request).await.unwrap();
            assert_ne!(count, 0);
            requests.fetch_add(1, Ordering::SeqCst);
            let response = if attempt == 0 {
                b"HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".as_slice()
            } else {
                b"HTTP/1.1 200 OK\r\nContent-Length: 11\r\nConnection: close\r\n\r\nhello world"
                    .as_slice()
            };
            socket.write_all(response).await.unwrap();
        }
    });
    let pool = DownloadPool::new(4.try_into().unwrap(), 16.try_into().unwrap()).unwrap();
    timeout(Duration::from_secs(4), async {
        let mut stream = pool.stream(&source).await.unwrap();
        let mut bytes = Vec::new();
        while let Some(chunk) = stream.try_next().await.unwrap() {
            bytes.extend_from_slice(chunk.as_ref());
        }
        assert_eq!(bytes, b"hello world");
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
    })
    .await
    .unwrap();
    server.await.unwrap();
}

#[tokio::test]
async fn retained_chunks_reserve_actual_bytes_and_allow_eof() {
    struct ChunkedTransport;
    impl HttpTransport for ChunkedTransport {
        async fn fetch(&self, request: Request<Buffer>) -> opendal::Result<Response<HttpBody>> {
            let chunks: Vec<Buffer> = if request.uri().path() == "/tiny" {
                ["a", "b", "c", "d", "e"]
                    .into_iter()
                    .map(Buffer::from)
                    .collect()
            } else {
                vec![Buffer::from("abcdefgh")]
            };
            let len = chunks.iter().map(Buffer::len).sum::<usize>();
            Ok(Response::builder()
                .status(StatusCode::OK)
                .header("content-length", len)
                .body(HttpBody::new(
                    stream::iter(chunks.into_iter().map(Ok)),
                    Some(len as u64),
                ))
                .unwrap())
        }
    }
    timeout(Duration::from_secs(5), async {
        let pool = pool_with(2, 4, StorageConfig::default(), ChunkedTransport);
        let mut download = pool.stream("https://chunks.invalid/tiny").await.unwrap();
        let mut held = Vec::new();
        for (index, expected) in b"abcd".iter().copied().enumerate() {
            let chunk = download.try_next().await.unwrap().unwrap();
            assert_eq!(chunk.as_ref(), [expected]);
            held.push(chunk);
            assert_eq!(pool.buffered_bytes(), index + 1);
        }
        assert!(
            timeout(Duration::from_millis(20), download.try_next())
                .await
                .is_err()
        );
        // Cancelling a quota wait and dropping its stream must not release
        // reservations still owned by consumers, or retain a new reservation.
        drop(download);
        assert_eq!(pool.buffered_bytes(), 4);
        held.pop();
        assert_eq!(pool.buffered_bytes(), 3);
        drop(held);
        assert_eq!(pool.buffered_bytes(), 0);

        let pool = pool_with(2, 3, StorageConfig::default(), ChunkedTransport);
        let mut download = pool.stream("https://chunks.invalid/large").await.unwrap();
        for expected in [b"abc", b"def"] {
            let chunk = download.try_next().await.unwrap().unwrap();
            assert_eq!(chunk.as_ref(), expected);
            assert_eq!(pool.buffered_bytes(), 3);
        }
        let tail = download.try_next().await.unwrap().unwrap();
        assert_eq!(tail.as_ref(), b"gh");
        assert_eq!(pool.buffered_bytes(), 2);
        // EOF needs no new transfer allocation, even with a retained tail.
        assert!(download.try_next().await.unwrap().is_none());
        drop(download);
        assert_eq!(pool.buffered_bytes(), 2);
        drop(tail);
        assert_eq!(pool.buffered_bytes(), 0);
    })
    .await
    .expect("exact download accounting stalled");
}

#[tokio::test]
async fn retained_chunk_does_not_pin_oversized_native_allocation() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    struct Allocation {
        bytes: Vec<u8>,
        dropped: Arc<AtomicUsize>,
    }
    impl AsRef<[u8]> for Allocation {
        fn as_ref(&self) -> &[u8] {
            &self.bytes
        }
    }
    impl Drop for Allocation {
        fn drop(&mut self) {
            self.dropped.fetch_add(1, Ordering::SeqCst);
        }
    }
    struct AllocatingTransport(Arc<AtomicUsize>);
    impl HttpTransport for AllocatingTransport {
        async fn fetch(&self, _: Request<Buffer>) -> opendal::Result<Response<HttpBody>> {
            let len = 4 * 1024 * 1024;
            let bytes = bytes::Bytes::from_owner(Allocation {
                bytes: vec![7; len],
                dropped: self.0.clone(),
            });
            Ok(Response::builder()
                .status(StatusCode::OK)
                .header("content-length", len)
                .body(HttpBody::new(
                    stream::iter([Ok(Buffer::from(bytes))]),
                    Some(len as u64),
                ))
                .unwrap())
        }
    }
    let dropped = Arc::new(AtomicUsize::new(0));
    let budget = 2 * 1024 * 1024;
    let pool = pool_with(
        2,
        budget,
        StorageConfig::default(),
        AllocatingTransport(dropped.clone()),
    );
    let mut download = pool
        .stream("https://chunks.invalid/oversized")
        .await
        .unwrap();
    let chunk = download.try_next().await.unwrap().unwrap();
    assert_eq!(chunk.as_ref().len(), budget);
    assert_eq!(pool.buffered_bytes(), budget);
    drop(download);
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
    assert_eq!(chunk.as_ref()[0], 7);
    assert_eq!(pool.buffered_bytes(), budget);
    drop(chunk);
    assert_eq!(pool.buffered_bytes(), 0);
}
