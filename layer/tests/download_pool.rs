use std::{sync::Arc, time::Duration};

use futures::TryStreamExt;
use open_arrow_sink_layer::DownloadPool;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    time::timeout,
};

const BODY: &[u8] = b"a payload larger than either pool budget";

async fn server() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    // The test runtime drops this task when the test ends.
    tokio::spawn(async move {
        loop {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = [0; 4096];
            let n = socket.read(&mut request).await.unwrap();
            let response = if request[..n].starts_with(b"GET /missing ") {
                "HTTP/1.1 404 Not Found\r\nContent-Length: 999999999\r\nConnection: close\r\n\r\n"
                    .to_owned()
            } else {
                format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    BODY.len(),
                    str::from_utf8(BODY).unwrap()
                )
            };
            let _ = socket.write_all(response.as_bytes()).await;
        }
    });
    url
}

fn pool(concurrency: usize, bytes: usize) -> Arc<DownloadPool> {
    Arc::new(DownloadPool::new(concurrency.try_into().unwrap(), bytes.try_into().unwrap()).unwrap())
}

// Run sequentially because all pools share the process-wide request limit.
#[tokio::test]
async fn streaming_limits_and_cleanup() {
    timeout(Duration::from_secs(5), async {
        let url = server().await;
        let first = pool(2, 8);
        let other = pool(64, 16);
        assert_eq!(first.concurrency(), other.concurrency());

        let mut streams = Vec::new();
        for _ in 0..first.concurrency() {
            streams.push(first.stream(&url).await.unwrap());
        }
        assert!(
            timeout(Duration::from_millis(20), other.stream(&url))
                .await
                .is_err()
        );
        let chunk = streams[0].try_next().await.unwrap().unwrap();
        drop(streams);

        // Dropping streams releases request slots, but retained chunks keep their budget.
        let mut resumed = first.stream(&url).await.unwrap();
        assert!(
            timeout(Duration::from_millis(20), resumed.try_next())
                .await
                .is_err()
        );
        drop(resumed);
        let mut independent = other.stream(&url).await.unwrap();
        let other_chunk = independent.try_next().await.unwrap().unwrap();
        assert_eq!(first.buffered_bytes(), 8);
        assert_eq!(other.buffered_bytes(), 16);
        drop((chunk, other_chunk, independent));
        assert_eq!(first.buffered_bytes(), 0);
        assert_eq!(other.buffered_bytes(), 0);

        let mut download = first.stream(&url).await.unwrap();
        let mut body = Vec::new();
        while let Some(chunk) = download.try_next().await.unwrap() {
            assert!(chunk.as_ref().len() <= 8);
            body.extend_from_slice(chunk.as_ref());
        }
        drop(download);
        assert_eq!(body, BODY);
        for source in ["not a URL", "file:///tmp/blob"] {
            assert!(first.stream(source).await.is_err());
        }
        let mut missing = first.stream(&format!("{url}/missing")).await.unwrap();
        assert!(missing.try_next().await.is_err());
        drop(missing);
        assert_eq!(first.buffered_bytes(), 0);
        assert!(first.stream(&url).await.is_ok());
    })
    .await
    .expect("download pool test stalled");
}
