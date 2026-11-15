#![cfg(feature = "download-blob")]

use futures::TryStreamExt;
use open_arrow_sink_layer::DownloadPool;
use std::{num::NonZeroUsize, sync::Arc, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    task::JoinHandle,
};

struct Server {
    url: String,
    task: JoinHandle<()>,
}
impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}
async fn server(response: Vec<u8>) -> Server {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let response = Arc::new(response);
    let task = tokio::spawn(async move {
        let mut handlers = tokio::task::JoinSet::new();
        loop {
            tokio::select! {
                connection = listener.accept() => {
                    let (mut socket, _) = connection.unwrap();
                    let response = response.clone();
                    handlers.spawn(async move {
                        let mut request = [0; 4096];
                        let _ = socket.read(&mut request).await;
                        let _ = socket.write_all(&response).await;
                    });
                }
                _ = handlers.join_next(), if !handlers.is_empty() => {}
            }
        }
    });
    Server { url, task }
}
fn response(size: usize) -> Vec<u8> {
    let mut response =
        format!("HTTP/1.1 200 OK\r\nContent-Length: {size}\r\nConnection: close\r\n\r\n")
            .into_bytes();
    response.resize(response.len() + size, b'x');
    response
}
fn pool(requests: usize, bytes: usize) -> Arc<DownloadPool> {
    Arc::new(
        DownloadPool::new(
            NonZeroUsize::new(requests).unwrap(),
            NonZeroUsize::new(bytes).unwrap(),
        )
        .unwrap(),
    )
}

#[tokio::test]
async fn all_streams_share_a_budget_until_consumers_drop_chunks() {
    let server = server(response(32)).await;
    let pool = pool(2, 8);
    let mut a = pool.stream(&server.url).await.unwrap();
    let mut b = pool.stream(&server.url).await.unwrap();
    let chunk = a.try_next().await.unwrap().unwrap();
    assert_eq!(chunk.as_ref(), b"xxxxxxxx");
    assert_eq!(pool.buffered_bytes(), 8);
    assert!(
        tokio::time::timeout(Duration::from_millis(20), b.try_next())
            .await
            .is_err()
    );
    assert_eq!(pool.buffered_bytes(), 8);
    drop(a);
    // The yielded chunk owns the reservation even after its source stream drops.
    assert_eq!(pool.buffered_bytes(), 8);
    drop(chunk);
    let next = tokio::time::timeout(Duration::from_secs(5), b.try_next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(next.as_ref().len(), 8);
    assert_eq!(pool.buffered_bytes(), 8);
    drop(next);
    drop(b);
    assert_eq!(pool.buffered_bytes(), 0);
}

#[tokio::test]
async fn dropping_stream_releases_request_slot_while_chunk_keeps_memory() {
    let server = server(response(32)).await;
    let pool = pool(1, 8);
    let mut first = pool.stream(&server.url).await.unwrap();
    let chunk = first.try_next().await.unwrap().unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(20), pool.stream(&server.url))
            .await
            .is_err()
    );
    drop(first);
    let mut second = tokio::time::timeout(Duration::from_secs(5), pool.stream(&server.url))
        .await
        .unwrap()
        .unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(20), second.try_next())
            .await
            .is_err()
    );
    drop(chunk);
    drop(second);
    assert_eq!(pool.buffered_bytes(), 0);
}

#[tokio::test]
async fn blob_larger_than_budget_streams_without_collecting_payload() {
    let size = 256 * 1024 + 7;
    let server = server(response(size)).await;
    let pool = pool(3, 1024);
    let mut download = pool.stream(&server.url).await.unwrap();
    let mut total = 0;
    while let Some(chunk) = download.try_next().await.unwrap() {
        assert!(chunk.as_ref().len() <= 1024);
        assert!(chunk.as_ref().iter().all(|&byte| byte == b'x'));
        assert_eq!(pool.buffered_bytes(), 1024);
        total += chunk.as_ref().len();
    }
    assert_eq!(total, size);
    assert_eq!(pool.buffered_bytes(), 0);
}

#[tokio::test]
async fn errors_release_budget_and_url_validation_precedes_requests() {
    let server = server(
        b"HTTP/1.1 404 Not Found\r\nContent-Length: 999999999\r\nConnection: close\r\n\r\n"
            .to_vec(),
    )
    .await;
    let pool = pool(1, 8);
    assert!(pool.stream("file:///tmp/blob").await.is_err());
    assert!(pool.stream("not a URL").await.is_err());
    let mut download = pool.stream(&server.url).await.unwrap();
    assert!(download.try_next().await.is_err());
    drop(download);
    assert_eq!(pool.buffered_bytes(), 0);
    assert!(
        tokio::time::timeout(Duration::from_secs(5), pool.stream(&server.url))
            .await
            .is_ok()
    );
}

#[tokio::test]
async fn timeout_cancels_stalled_body_and_returns_reservation() {
    use opendal::layers::TimeoutLayer;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let task = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut request = [0; 4096];
        let _ = socket.read(&mut request).await;
        socket
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 32\r\n\r\n")
            .await
            .unwrap();
        std::future::pending::<()>().await;
    });
    let _server = Server {
        url: url.clone(),
        task,
    };
    let pool = Arc::new(
        DownloadPool::new(NonZeroUsize::new(1).unwrap(), NonZeroUsize::new(8).unwrap())
            .unwrap()
            .with_layer(TimeoutLayer::default().with_io_timeout(Duration::from_millis(50))),
    );
    let mut download = pool.stream(&url).await.unwrap();
    assert!(
        tokio::time::timeout(Duration::from_secs(5), download.try_next())
            .await
            .unwrap()
            .is_err()
    );
    drop(download);
    assert_eq!(pool.buffered_bytes(), 0);
}

#[tokio::test]
async fn opendal_retry_policy_is_explicit_and_preserves_signed_urls() {
    use opendal::layers::{RetryLayer, TimeoutLayer};
    for retry in [false, true] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!(
            "http://{}/a//b%2Fc?token=a%2Fb&x=1+2#fragment",
            listener.local_addr().unwrap()
        );
        let count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let attempts = count.clone();
        let task = tokio::spawn(async move {
            loop {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut request = Vec::new();
                let mut buffer = [0; 1024];
                while !request.windows(4).any(|w| w == b"\r\n\r\n") {
                    let n = socket.read(&mut buffer).await.unwrap();
                    assert_ne!(n, 0);
                    request.extend_from_slice(&buffer[..n]);
                }
                assert!(request.starts_with(b"GET /a//b%2Fc?token=a%2Fb&x=1+2 HTTP/1.1\r\n"));
                let attempt = attempts.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                let response = if attempt == 0 {
                    b"HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".as_slice()
                } else {
                    b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok"
                };
                socket.write_all(response).await.unwrap();
            }
        });
        let server = Server { url, task };
        let mut configured =
            DownloadPool::new(NonZeroUsize::new(1).unwrap(), NonZeroUsize::new(8).unwrap())
                .unwrap();
        if retry {
            configured = configured
                .with_layer(TimeoutLayer::default().with_io_timeout(Duration::from_secs(1)))
                .with_layer(
                    RetryLayer::default()
                        .with_min_delay(Duration::from_millis(1))
                        .with_max_delay(Duration::from_millis(1))
                        .with_max_times(1),
                );
        }
        let pool = Arc::new(configured);
        let mut download = pool.stream(&server.url).await.unwrap();
        let result = async {
            let mut value = Vec::new();
            while let Some(chunk) = download.try_next().await? {
                value.extend_from_slice(chunk.as_ref());
            }
            Ok::<_, open_arrow_sink_core::Error>(value)
        }
        .await;
        if retry {
            assert_eq!(result.unwrap(), b"ok");
        } else {
            assert!(result.is_err());
        }
        assert_eq!(
            count.load(std::sync::atomic::Ordering::SeqCst),
            if retry { 2 } else { 1 }
        );
        assert_eq!(pool.buffered_bytes(), 0);
    }
}
