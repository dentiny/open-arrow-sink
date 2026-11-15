use futures::{poll, stream};
use http::{Request, Response};
use open_arrow_sink_storage::{HttpConcurrencyLimitLayer, Storage, opendal};
use opendal::raw::oio::ReadStream;
use opendal::{Buffer, HttpBody, HttpTransport, HttpTransporter};

#[derive(Clone)]
struct Transport;
impl HttpTransport for Transport {
    async fn fetch(&self, request: Request<Buffer>) -> opendal::Result<Response<HttpBody>> {
        match request.uri().path() {
            "/fetch-error" => Err(opendal::Error::new(
                opendal::ErrorKind::Unexpected,
                "fetch failed",
            )),
            "/pending" => std::future::pending().await,
            "/body-error" => Ok(Response::new(HttpBody::new(
                stream::iter(vec![Err(opendal::Error::new(
                    opendal::ErrorKind::Unexpected,
                    "body failed",
                ))]),
                None,
            ))),
            _ => Ok(Response::new(HttpBody::new(
                stream::iter(vec![Ok(Buffer::from("data"))]),
                None,
            ))),
        }
    }
}
fn request(path: &str) -> Request<Buffer> {
    Request::builder().uri(path).body(Buffer::new()).unwrap()
}

fn operator() -> opendal::Operator {
    let operator = opendal::Operator::new(opendal::services::Fs::default().root("/")).unwrap();
    let context = operator
        .base_context()
        .clone()
        .with_http_transport(HttpTransporter::new(Transport));
    operator.with_context(context)
}

fn storage() -> Storage {
    Storage::new(operator(), "file:///".parse().unwrap()).unwrap()
}

#[tokio::test]
async fn replacing_storage_budget_preserves_caller_layers() {
    let operator = operator().layer(HttpConcurrencyLimitLayer::new(1).unwrap());
    let storage = Storage::new(operator, "file:///".parse().unwrap())
        .unwrap()
        .with_http_concurrency_limit(HttpConcurrencyLimitLayer::new(2).unwrap())
        .with_http_concurrency_limit(HttpConcurrencyLimitLayer::new(3).unwrap());
    let transport = storage.operator().context().http_transport().clone();
    let response = transport.fetch(request("/")).await.unwrap();
    // The caller's independent one-request budget still applies.
    let mut waiting = Box::pin(transport.fetch(request("/")));
    assert!(poll!(waiting.as_mut()).is_pending());
    drop(response);
    assert!(poll!(waiting.as_mut()).is_ready());
}

#[tokio::test]
async fn storage_operators_and_clones_share_budget() {
    let storage = storage().with_http_concurrency_limit(HttpConcurrencyLimitLayer::new(1).unwrap());
    let first = storage.operator().context().http_transport().clone();
    let second = storage
        .clone()
        .operator()
        .context()
        .http_transport()
        .clone();
    let response = first.fetch(request("/")).await.unwrap();
    let mut waiting = Box::pin(second.fetch(request("/")));
    assert!(poll!(waiting.as_mut()).is_pending());
    drop(response);
    assert!(poll!(waiting.as_mut()).is_ready());
}

#[tokio::test]
async fn replacing_storage_budget_does_not_nest_limiters() {
    let budget = HttpConcurrencyLimitLayer::new(1).unwrap();
    // Applying the same budget twice must not acquire it twice per request.
    let storage = storage()
        .with_http_concurrency_limit(budget.clone())
        .with_http_concurrency_limit(budget);
    let old = storage.operator().context().http_transport().clone();
    let response = tokio::time::timeout(std::time::Duration::from_secs(1), old.fetch(request("/")))
        .await
        .unwrap()
        .unwrap();
    // A replacement budget is independent of already-cloned operators.
    let replacement =
        storage.with_http_concurrency_limit(HttpConcurrencyLimitLayer::new(2).unwrap());
    let transport = replacement.operator().context().http_transport().clone();
    let first = tokio::time::timeout(
        std::time::Duration::from_secs(1),
        transport.fetch(request("/")),
    )
    .await
    .unwrap()
    .unwrap();
    let second = tokio::time::timeout(
        std::time::Duration::from_secs(1),
        transport.fetch(request("/")),
    )
    .await
    .unwrap()
    .unwrap();
    let mut third = Box::pin(transport.fetch(request("/")));
    assert!(poll!(third.as_mut()).is_pending());
    drop(first);
    assert!(poll!(third.as_mut()).is_ready());
    drop((second, response));
}

#[tokio::test]
async fn cloned_budgets_hold_capacity_until_body_drop() {
    let budget = HttpConcurrencyLimitLayer::new(1).unwrap();
    let first = budget.wrap(HttpTransporter::new(Transport));
    let second = budget.clone().wrap(HttpTransporter::new(Transport));
    let response = first.fetch(request("/")).await.unwrap();
    let mut waiting = Box::pin(second.fetch(request("/")));
    assert!(poll!(waiting.as_mut()).is_pending());
    drop(response);
    assert!(poll!(waiting.as_mut()).is_ready());
}

#[tokio::test]
async fn eof_errors_and_cancellation_release_capacity() {
    let transport = HttpConcurrencyLimitLayer::new(1)
        .unwrap()
        .wrap(HttpTransporter::new(Transport));
    for path in ["/", "/body-error"] {
        let mut response = transport.fetch(request(path)).await.unwrap();
        let mut waiting = Box::pin(transport.fetch(request("/")));
        assert!(poll!(waiting.as_mut()).is_pending());
        if path == "/" {
            assert!(!response.body_mut().read().await.unwrap().is_empty());
            assert!(poll!(waiting.as_mut()).is_pending());
            assert!(response.body_mut().read().await.unwrap().is_empty());
        } else {
            assert!(response.body_mut().read().await.is_err());
        }
        assert!(poll!(waiting.as_mut()).is_ready());
    }
    assert!(transport.fetch(request("/fetch-error")).await.is_err());
    {
        let mut cancelled = Box::pin(transport.fetch(request("/pending")));
        assert!(poll!(cancelled.as_mut()).is_pending());
    }
    assert!(transport.fetch(request("/")).await.is_ok());
}

#[test]
fn invalid_budgets_are_rejected() {
    assert!(HttpConcurrencyLimitLayer::new(0).is_err());
    assert!(HttpConcurrencyLimitLayer::new(tokio::sync::Semaphore::MAX_PERMITS + 1).is_err());
}
