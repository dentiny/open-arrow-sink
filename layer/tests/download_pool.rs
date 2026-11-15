use std::{num::NonZeroUsize, sync::Arc, time::Duration};

use futures::TryStreamExt;
use open_arrow_sink_layer::DownloadPool;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    task::JoinHandle,
    time::timeout,
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
    let task = tokio::spawn(async move {
        loop {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = [0; 4096];
            let _ = socket.read(&mut request).await;
            let _ = socket.write_all(&response).await;
        }
    });
    Server { url, task }
}

fn pool(concurrency: usize, bytes: usize) -> Arc<DownloadPool> {
    Arc::new(
        DownloadPool::new(
            NonZeroUsize::new(concurrency).unwrap(),
            NonZeroUsize::new(bytes).unwrap(),
        )
        .unwrap(),
    )
}

// One sequential test keeps the process-wide limiter consistent without subprocesses.
#[tokio::test]
async fn download_pool_streams_with_shared_limits() {
    timeout(Duration::from_secs(5), check_download_pool())
        .await
        .expect("download pool test stalled");
}

async fn check_download_pool() {
    let size = 128 * 1024 + 7;
    let mut response =
        format!("HTTP/1.1 200 OK\r\nContent-Length: {size}\r\nConnection: close\r\n\r\n")
            .into_bytes();
    response.resize(response.len() + size, b'x');
    let source = server(response).await;
    let first = pool(2, 1024);
    let other = pool(64, 2048);
    assert_eq!(other.concurrency(), first.concurrency());

    // Fill every process-wide request slot, even if the test has an env override.
    let mut streams = Vec::new();
    for _ in 0..first.concurrency() {
        streams.push(first.stream(&source.url).await.unwrap());
    }
    let mut download = streams.pop().unwrap();
    let chunk = download.try_next().await.unwrap().unwrap();
    assert_eq!(first.buffered_bytes(), 1024);
    assert!(
        timeout(Duration::from_millis(20), other.stream(&source.url))
            .await
            .is_err()
    );

    // Cancellation releases a request slot; the yielded chunk keeps its budget.
    drop(download);
    let mut resumed = first.stream(&source.url).await.unwrap();
    assert!(
        timeout(Duration::from_millis(20), resumed.try_next())
            .await
            .is_err()
    );
    drop(resumed);
    let mut independent = other.stream(&source.url).await.unwrap();
    let other_chunk = independent.try_next().await.unwrap().unwrap();
    assert_eq!(first.buffered_bytes(), 1024);
    assert_eq!(other.buffered_bytes(), 2048);
    drop(other_chunk);
    drop(independent);
    drop(chunk);
    drop(streams);
    assert_eq!(first.buffered_bytes(), 0);
    assert_eq!(other.buffered_bytes(), 0);

    // A payload larger than the budget is read incrementally and completely.
    let mut download = first.stream(&source.url).await.unwrap();
    let mut total = 0;
    while let Some(chunk) = download.try_next().await.unwrap() {
        assert!(chunk.as_ref().len() <= 1024);
        assert!(chunk.as_ref().iter().all(|&byte| byte == b'x'));
        total += chunk.as_ref().len();
    }
    drop(download);
    assert_eq!(total, size);
    assert_eq!(first.buffered_bytes(), 0);

    for url in ["not a URL", "file:///tmp/blob"] {
        assert!(first.stream(url).await.is_err());
    }
    let missing = server(
        b"HTTP/1.1 404 Not Found\r\nContent-Length: 999999999\r\nConnection: close\r\n\r\n"
            .to_vec(),
    )
    .await;
    let mut download = first.stream(&missing.url).await.unwrap();
    assert!(download.try_next().await.is_err());
    drop(download);
    assert_eq!(first.buffered_bytes(), 0);
    assert!(first.stream(&source.url).await.is_ok());
}
