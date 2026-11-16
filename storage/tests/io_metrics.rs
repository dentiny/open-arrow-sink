use open_arrow_sink_core::{IoMetrics, IoSnapshot, WriteOptions};
use open_arrow_sink_storage::{Storage, opendal};

#[tokio::test]
async fn storage_counts_read_and_write_bytes_once() {
    let metrics = IoMetrics::default();
    let storage = Storage::new(
        opendal::Operator::new(opendal::services::Memory::default()).unwrap(),
        "memory:///".parse().unwrap(),
    )
    .unwrap()
    .with_io_metrics(metrics.clone())
    .with_write_options(&WriteOptions::default().with_io_metrics(metrics.clone()));
    let path = storage.resolve(&storage.url("payload").unwrap()).unwrap();

    storage.write(&path, "abcdef".into()).await.unwrap();
    assert_eq!(storage.read_range(&path, 1..4).await.unwrap(), "bcd");
    assert_eq!(
        metrics.snapshot(),
        IoSnapshot {
            bytes_read: 3,
            bytes_written: 6
        }
    );
}

#[rstest::rstest]
#[case::observer_before_policy(true)]
#[case::observer_after_policy(false)]
#[tokio::test]
async fn retry_and_reconfiguration_count_successful_payload_once(#[case] observe_first: bool) {
    use futures::stream;
    use http::{Request, Response, StatusCode};
    use open_arrow_sink_storage::{HttpConcurrencyLimitLayer, RetryConfig, StorageConfig};
    use opendal::{Buffer, HttpBody, HttpTransport, HttpTransporter};
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    use std::time::Duration;

    struct Transient(Arc<AtomicUsize>);
    impl HttpTransport for Transient {
        async fn fetch(&self, _: Request<Buffer>) -> opendal::Result<Response<HttpBody>> {
            let status = if self.0.fetch_add(1, Ordering::SeqCst) == 0 {
                StatusCode::SERVICE_UNAVAILABLE
            } else {
                StatusCode::OK
            };
            Ok(Response::builder()
                .status(status)
                .header("content-length", 6)
                .body(HttpBody::new(
                    stream::iter([Ok(Buffer::from("abcdef"))]),
                    Some(6),
                ))
                .unwrap())
        }
    }
    let first = IoMetrics::default();
    let second = IoMetrics::default();
    let attempts = Arc::new(AtomicUsize::new(0));
    let mut storage = Storage::new(
        opendal::Operator::new(
            opendal::services::Http::default().endpoint("https://metrics.invalid"),
        )
        .unwrap(),
        "https://metrics.invalid/".parse().unwrap(),
    )
    .unwrap();
    if observe_first {
        storage = storage.with_io_metrics(first.clone());
    }
    storage = storage
        .with_config(StorageConfig {
            retry: Some(RetryConfig {
                max_retries: 1,
                min_delay: Duration::from_millis(1),
                max_delay: Duration::from_millis(1),
                jitter: false,
                ..Default::default()
            }),
            ..Default::default()
        })
        .unwrap()
        .with_http_transport(HttpTransporter::new(Transient(attempts.clone())))
        .with_http_concurrency_limit(HttpConcurrencyLimitLayer::new(2).unwrap())
        .with_io_metrics(first.clone())
        .with_io_metrics(second.clone());
    let old_operator = storage.operator();
    // Rebuild from the raw inputs, preserving observers without stacking them again.
    storage = storage
        .with_config(StorageConfig::default())
        .unwrap()
        .with_http_concurrency_limit(HttpConcurrencyLimitLayer::new(3).unwrap());
    let path = storage.resolve(&storage.url("payload").unwrap()).unwrap();
    assert_eq!(storage.read(&path).await.unwrap(), "abcdef");
    assert_eq!(attempts.load(Ordering::SeqCst), 2);
    assert_eq!(first.snapshot().bytes_read, 6);
    assert_eq!(second.snapshot(), first.snapshot());
    assert_eq!(
        old_operator.read("payload").await.unwrap().to_bytes(),
        "abcdef"
    );
    assert_eq!(attempts.load(Ordering::SeqCst), 3);
    assert_eq!(first.snapshot().bytes_read, 12);
    assert_eq!(second.snapshot(), first.snapshot());
}
