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
        let pool = DownloadPool::new(2.try_into().unwrap(), 4.try_into().unwrap())
            .unwrap()
            .with_http_transport(HttpTransporter::new(ChunkedTransport));
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

        let pool = DownloadPool::new(2.try_into().unwrap(), 3.try_into().unwrap())
            .unwrap()
            .with_http_transport(HttpTransporter::new(ChunkedTransport));
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
    let pool = DownloadPool::new(2.try_into().unwrap(), budget.try_into().unwrap())
        .unwrap()
        .with_http_transport(HttpTransporter::new(AllocatingTransport(dropped.clone())));
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

        {
            let mut download = first.stream(url).await.unwrap();
            let mut body = Vec::new();
            while let Some(chunk) = download.try_next().await.unwrap() {
                assert_le!(chunk.as_ref().len(), 8);
                body.extend_from_slice(chunk.as_ref());
            }
            assert_eq!(body, BODY);
        }
        for source in ["not a URL", "unsupported:///tmp/blob"] {
            assert!(first.stream(source).await.is_err());
        }
        {
            let mut missing = first.stream(&format!("{url}/missing")).await.unwrap();
            assert!(missing.try_next().await.is_err());
        }
        assert_eq!(first.buffered_bytes(), 0);
        assert!(first.stream(url).await.is_ok());

        {
            // Replacing the transport after layering preserves retry policy.
            use std::sync::atomic::{AtomicUsize, Ordering};
            struct Transient(Arc<AtomicUsize>);
            impl HttpTransport for Transient {
                async fn fetch(
                    &self,
                    request: Request<Buffer>,
                ) -> opendal::Result<Response<HttpBody>> {
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
        }
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
