use futures::{poll, stream};
use http::{Request, Response, StatusCode};
use open_arrow_sink_core::{IoMetrics, IoObservers, Result, async_trait};
use open_arrow_sink_storage::{
    BlobDownloader, DownloadContext, DownloadStream, Downloader, HttpConcurrencyLimitLayer,
    ObjectPath, OpenDalDownloader, RetryConfig, StorageConfig, opendal,
};
use open_arrow_sink_test_support::within;
use opendal::{Buffer, HttpBody, HttpTransport, HttpTransporter};
use std::{sync::Arc, time::Duration};

const URL: &str = "https://download-fixture.invalid";
const BODY: &[u8] = b"a payload larger than one download chunk";

struct Transport;
impl HttpTransport for Transport {
    async fn fetch(&self, request: Request<Buffer>) -> opendal::Result<Response<HttpBody>> {
        let missing = request.uri().path() == "/missing";
        Ok(Response::builder()
            .status(if missing {
                StatusCode::NOT_FOUND
            } else {
                StatusCode::OK
            })
            .header(
                "content-length",
                if missing { 999999999 } else { BODY.len() },
            )
            .body(if missing {
                // Diagnostics must discard an endless error body.
                HttpBody::new(stream::pending(), Some(999999999))
            } else {
                HttpBody::new(
                    stream::iter([Ok(Buffer::from(BODY))]),
                    Some(BODY.len() as u64),
                )
            })
            .unwrap())
    }
}
fn pool(concurrency: usize) -> BlobDownloader {
    BlobDownloader::new(concurrency.try_into().unwrap()).unwrap()
}
fn default_pool() -> BlobDownloader {
    pool(2).with_downloader(Arc::new(
        OpenDalDownloader::new(StorageConfig::default())
            .unwrap()
            .with_http_transport(HttpTransporter::new(Transport)),
    ))
}

#[tokio::test]
async fn failures_release_slots() {
    let downloads = default_pool();
    for url in [
        "not a URL",
        "unsupported:///tmp/blob",
        &format!("{URL}/missing"),
    ] {
        assert!(downloads.download(url).await.is_err());
    }
    assert_eq!(downloads.download(URL).await.unwrap().as_ref(), BODY);
}

#[tokio::test]
async fn retry_policy_applies_to_the_provider_transport() {
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
    let source = OpenDalDownloader::new(StorageConfig {
        retry: Some(RetryConfig {
            max_retries: 1,
            min_delay: Duration::from_millis(1),
            max_delay: Duration::from_millis(1),
            ..Default::default()
        }),
        ..Default::default()
    })
    .unwrap()
    .with_http_transport(HttpTransporter::new(Transient(attempts.clone())));
    let downloads = pool(2).with_downloader(Arc::new(source));
    assert_eq!(downloads.download(URL).await.unwrap().as_ref(), BODY);
    assert_eq!(attempts.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn filesystem_download_round_trips() {
    let temp = tempfile::tempdir().unwrap();
    let file = temp.path().join("source.bin");
    tokio::fs::write(&file, BODY).await.unwrap();
    let url = open_arrow_sink_storage::Url::from_file_path(file).unwrap();
    let downloads = pool(1);
    assert_eq!(
        downloads.download(url.as_str()).await.unwrap().as_ref(),
        BODY
    );
}

#[tokio::test]
async fn shared_http_permits_cover_bodies_and_return_on_cancellation() {
    use futures::StreamExt;
    struct Held;
    impl HttpTransport for Held {
        async fn fetch(&self, _: Request<Buffer>) -> opendal::Result<Response<HttpBody>> {
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
    within(async {
        let budget = HttpConcurrencyLimitLayer::new(1).unwrap();
        let make = || {
            let storage = OpenDalDownloader::http_storage(StorageConfig::default())
                .unwrap()
                .with_http_concurrency_limit(budget.clone())
                .with_http_transport(HttpTransporter::new(Held));
            pool(2)
                .with_http_concurrency_limit(budget.clone())
                .with_downloader(Arc::new(
                    OpenDalDownloader::new(StorageConfig::default())
                        .unwrap()
                        .with_http_storage(storage)
                        .unwrap(),
                ))
        };
        let first = make();
        let second = make();
        let a = IoMetrics::default();
        let b = IoMetrics::default();
        let a_observers = IoObservers::from(a.clone());
        let b_observers = IoObservers::from(b.clone());
        let mut held = Box::pin(first.download_all(&[URL], Some(&a_observers)));
        let mut waiting = Box::pin(second.download_all(&[URL], Some(&b_observers)));
        assert!(poll!(&mut held).is_pending());
        assert_eq!(a.snapshot().bytes_read, BODY.len() as u64);
        assert!(poll!(&mut waiting).is_pending());
        assert_eq!(b.snapshot().bytes_read, 0);
        drop(held);

        assert!(poll!(&mut waiting).is_pending());
        assert_eq!(b.snapshot().bytes_read, BODY.len() as u64);
        drop(waiting);
    })
    .await;
}

#[tokio::test]
async fn custom_downloader_keeps_chunk_contract_and_releases_http_at_eof() {
    struct Custom;
    #[async_trait]
    impl Downloader for Custom {
        async fn open(&self, source: &str, context: DownloadContext) -> Result<DownloadStream> {
            let permit = context.http_requests().acquire().await;
            let data = if source.ends_with("too-large") {
                bytes::Bytes::from(vec![0; context.chunk_bytes() + 1])
            } else {
                bytes::Bytes::from_static(b"custom")
            };
            Ok(Box::pin(
                async_stream::try_stream! { let _permit = permit; yield data; },
            ))
        }
    }
    within(async {
        let downloads = pool(2)
            .with_downloader(Arc::new(Custom))
            .with_http_concurrency_limit(HttpConcurrencyLimitLayer::new(1).unwrap());
        let a = downloads.download("custom://a").await.unwrap();
        // Holding a payload must not retain an HTTP permit.
        let b = downloads.download("custom://b").await.unwrap();
        assert_eq!(a.as_ref(), b"custom");
        assert_eq!(b.as_ref(), b"custom");

        drop((a, b));
        assert!(downloads.download("custom://too-large").await.is_err());

        assert_eq!(
            downloads
                .download("custom://after-error")
                .await
                .unwrap()
                .as_ref(),
            b"custom"
        );
    })
    .await;
}

#[tokio::test]
async fn shared_source_observers_count_each_download_once() {
    let shared = IoMetrics::default();
    let independent = IoMetrics::default();
    let observers = IoObservers::from(shared.clone()).combine(independent.clone());
    let memory = open_arrow_sink_test_support::memory_storage("memory://source/");
    let url = memory.url(&ObjectPath::parse("blob").unwrap()).unwrap();
    memory
        .write(&memory.path(&url).unwrap(), BODY.into())
        .await
        .unwrap();
    let http = OpenDalDownloader::http_storage(StorageConfig::default())
        .unwrap()
        .with_http_transport(HttpTransporter::new(Transport))
        .with_io_metrics(observers.clone());
    let downloads = [
        (
            url.as_str(),
            pool(1).with_source_storage(memory.with_io_metrics(observers)),
        ),
        (
            URL,
            pool(1).with_downloader(Arc::new(
                OpenDalDownloader::new(StorageConfig::default())
                    .unwrap()
                    .with_http_storage(http)
                    .unwrap(),
            )),
        ),
    ];
    let session = IoObservers::from(shared.clone());
    for (index, (url, downloads)) in downloads.into_iter().enumerate() {
        assert_eq!(
            downloads
                .download_all(&[url], Some(&session))
                .await
                .unwrap()[0]
                .as_ref(),
            BODY
        );
        let expected = (index as u64 + 1) * BODY.len() as u64;
        assert_eq!(shared.snapshot().bytes_read, expected);
        assert_eq!(independent.snapshot().bytes_read, expected);
    }
}

#[tokio::test]
async fn data_uris_decode_and_reject_invalid_inputs() {
    let downloads = pool(1);
    for uri in [
        "data:application/octet-stream;base64,AP8%3D",
        "DaTa:application/octet-stream;base64,AP8%3D",
        "data:,%00%ff",
    ] {
        assert_eq!(downloads.download(uri).await.unwrap().as_ref(), &[0, 255]);
    }
    for uri in ["data:missing-comma", "data:;base64,????"] {
        assert!(downloads.download(uri).await.is_err(), "{uri}");
    }
    assert_eq!(downloads.download("data:,").await.unwrap().as_ref(), b"");
}

#[tokio::test]
async fn registered_http_sources_apply_scoped_credentials_and_preserve_queries() {
    struct ScopedTransport(bool);
    impl HttpTransport for ScopedTransport {
        async fn fetch(&self, request: Request<Buffer>) -> opendal::Result<Response<HttpBody>> {
            assert_eq!(
                request
                    .headers()
                    .get("authorization")
                    .map(|v| v.to_str().unwrap()),
                self.0.then_some("Bearer source-token")
            );
            assert_eq!(request.uri().query(), Some("signature=keep"));
            assert_eq!(request.uri().path().starts_with("/runs/"), self.0);
            Transport.fetch(request).await
        }
    }
    let source = open_arrow_sink_storage::Storage::new(
        opendal::Operator::new(
            opendal::services::Http::default()
                .endpoint(URL)
                .root("/runs/")
                .token("source-token"),
        )
        .unwrap(),
        format!("{URL}/runs/").parse().unwrap(),
    )
    .unwrap()
    .with_http_transport(HttpTransporter::new(ScopedTransport(true)));
    let downloads = pool(1)
        .with_downloader(Arc::new(
            OpenDalDownloader::new(StorageConfig::default())
                .unwrap()
                .with_http_transport(HttpTransporter::new(ScopedTransport(false))),
        ))
        .with_source_storage(source);
    for path in ["runs/media", "outside/media"] {
        assert_eq!(
            downloads
                .download(&format!("{URL}/{path}?signature=keep#ignored"))
                .await
                .unwrap()
                .as_ref(),
            BODY
        );
    }
}

#[tokio::test]
async fn data_urls_follow_standard_decoding_rules() {
    let downloads = pool(1);
    for (source, expected) in [
        ("data:,hello#ignored", &b"hello"[..]),
        ("data:;base64,aGk=#ignored", &b"hi"[..]),
        ("data:;BASE64,aGk=", &b"hi"[..]),
        ("data:;base64,aGk", &b"hi"[..]),
        ("data:; base64 ,aG%20k%3D", &b"hi"[..]),
        ("data:,hello%23world", &b"hello#world"[..]),
        ("data:,hello?world", &b"hello?world"[..]),
        (" \tDa\nta:,%00%FF", &[0, 255][..]),
    ] {
        assert_eq!(
            downloads.download(source).await.unwrap().as_ref(),
            expected,
            "{source}"
        );
    }
    for source in [
        "data:#fragment,not-a-body",
        "data:;base64,A",
        "data:;base64,a===",
    ] {
        assert!(downloads.download(source).await.is_err(), "{source}");
    }
}
