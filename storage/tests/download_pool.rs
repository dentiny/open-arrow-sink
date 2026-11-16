#![cfg(feature = "http")]

use std::{sync::Arc, time::Duration};

use futures::{TryStreamExt, stream};
use http::{Request, Response, StatusCode};
use more_asserts::assert_le;
use open_arrow_sink_storage::{DownloadPool, HttpConcurrencyLimitLayer, opendal};
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
async fn streaming_limits_and_cleanup() {
    timeout(Duration::from_secs(5), async {
        let url = "https://download-fixture.invalid";
        let first = pool(2, 8);
        let other = pool(64, 16);
        assert_eq!(first.concurrency(), 2);
        assert_eq!(other.concurrency(), 64);

        let mut streams = Vec::new();
        for _ in 0..first.concurrency() {
            streams.push(first.stream(url).await.unwrap());
        }
        assert!(
            timeout(Duration::from_millis(20), first.stream(url))
                .await
                .is_err()
        );
        // An unrelated pool remains usable when the first pool is saturated.
        let mut independent = other.stream(url).await.unwrap();
        let other_chunk = independent.try_next().await.unwrap().unwrap();
        drop((independent, other_chunk));
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
        let url = "https://download-fixture.invalid";
        let mut held = first.stream(url).await.unwrap();
        let chunk = held.try_next().await.unwrap().unwrap();
        let mut waiting = second.stream(url).await.unwrap();
        assert!(
            timeout(Duration::from_millis(20), waiting.try_next())
                .await
                .is_err()
        );
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
fn invalid_concurrency_does_not_initialize_or_change_other_pools() {
    assert!(
        DownloadPool::new(
            (tokio::sync::Semaphore::MAX_PERMITS + 1)
                .try_into()
                .unwrap(),
            8.try_into().unwrap(),
        )
        .is_err()
    );
    assert_eq!(
        DownloadPool::new(3.try_into().unwrap(), 8.try_into().unwrap())
            .unwrap()
            .concurrency(),
        3
    );
    assert_eq!(DownloadPool::with_defaults().unwrap().concurrency(), 64);
}
