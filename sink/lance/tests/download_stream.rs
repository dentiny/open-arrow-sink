use std::{num::NonZeroUsize, sync::Arc, time::Duration};

use arrow_array::{Array, ArrayRef, Int32Array, LargeBinaryArray, RecordBatch, StringArray};
use lance::{Dataset, datatypes::BlobHandling};
use open_arrow_sink_core::{Ordering, Sink, SinkExt, WriteOptions};
use open_arrow_sink_lance::LanceDatasetSink;
use open_arrow_sink_layer::{DownloadBlobLayer, DownloadPool};
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
async fn server() -> Server {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let task = tokio::spawn(async move {
        let mut handlers = tokio::task::JoinSet::new();
        loop {
            tokio::select! {
                connection = listener.accept() => {
                    let (mut socket, _) = connection.unwrap();
                    handlers.spawn(async move {
                        let mut request = Vec::new();
                        let mut buffer = [0; 4096];
                        while !request.windows(4).any(|w| w == b"\r\n\r\n") {
                            let count = socket.read(&mut buffer).await.unwrap();
                            if count == 0 { return; }
                            request.extend_from_slice(&buffer[..count]);
                        }
                        let request = String::from_utf8(request).unwrap();
                        let path = request.split_whitespace().nth(1).unwrap();
                        if path == "/missing" {
                            let _ = socket.write_all(b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").await;
                            return;
                        }
                        let size = if path == "/empty" { 0 } else { 17 * 1024 * 1024 };
                        let headers = format!("HTTP/1.1 200 OK\r\nContent-Length: {size}\r\nConnection: close\r\n\r\n");
                        if socket.write_all(headers.as_bytes()).await.is_err() { return; }
                        if path == "/stall" { std::future::pending::<()>().await; }
                        let chunk = vec![b'x'; 64 * 1024];
                        for _ in 0..size / chunk.len() {
                            if socket.write_all(&chunk).await.is_err() { return; }
                        }
                    });
                }
                _ = handlers.join_next(), if !handlers.is_empty() => {}
            }
        }
    });
    Server { url, task }
}
fn options() -> WriteOptions {
    WriteOptions {
        ordering: Ordering::Unordered,
        ..Default::default()
    }
}
fn pool() -> Arc<DownloadPool> {
    Arc::new(
        DownloadPool::new(
            NonZeroUsize::new(2).unwrap(),
            NonZeroUsize::new(1024).unwrap(),
        )
        .unwrap(),
    )
}

#[tokio::test]
async fn downloads_larger_than_budget_round_trip_as_owned_native_blobs() {
    let server = server().await;
    let large = format!("{}/large", server.url);
    let empty = format!("{}/empty", server.url);
    let input = RecordBatch::try_from_iter([
        (
            "url",
            Arc::new(StringArray::from(vec![Some(large), None, Some(empty)])) as ArrayRef,
        ),
        ("id", Arc::new(Int32Array::from(vec![0, 1, 2])) as ArrayRef),
    ])
    .unwrap();
    let budget = pool();
    let sink = LanceDatasetSink.layer(
        DownloadBlobLayer::new(["url".to_owned()].into_iter().collect())
            .with_download_pool(budget.clone()),
    );
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("dataset");
    let mut writer = sink
        .open(path.clone(), input.schema(), options())
        .await
        .unwrap();
    writer.write(input).await.unwrap();
    assert_eq!(budget.buffered_bytes(), 0);
    assert_eq!(writer.commit().await.unwrap().rows_written, 3);
    drop(server);
    let dataset = Dataset::open(path.to_str().unwrap()).await.unwrap();
    let actual = dataset
        .scan()
        .blob_handling(BlobHandling::AllBinary)
        .try_into_batch()
        .await
        .unwrap();
    let values = actual
        .column(0)
        .as_any()
        .downcast_ref::<LargeBinaryArray>()
        .unwrap();
    let ids = actual
        .column(1)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    for row in 0..actual.num_rows() {
        match ids.value(row) {
            0 => {
                assert_eq!(values.value(row).len(), 17 * 1024 * 1024);
                assert!(values.value(row).iter().all(|&byte| byte == b'x'));
            }
            1 => assert!(values.is_null(row)),
            2 => {
                assert!(!values.is_null(row));
                assert!(values.value(row).is_empty());
            }
            _ => unreachable!(),
        }
    }
}

#[tokio::test]
async fn download_errors_and_cancellation_abort_native_storage() {
    let server = server().await;
    for path in ["/missing", "/stall"] {
        let url = format!("{}{path}", server.url);
        let input = RecordBatch::try_from_iter([(
            "url",
            Arc::new(StringArray::from(vec![url])) as ArrayRef,
        )])
        .unwrap();
        let budget = pool();
        let sink = LanceDatasetSink.layer(
            DownloadBlobLayer::new(["url".to_owned()].into_iter().collect())
                .with_download_pool(budget.clone()),
        );
        let temp = tempfile::tempdir().unwrap();
        let destination = temp.path().join("dataset");
        let mut writer = sink
            .open(destination.clone(), input.schema(), options())
            .await
            .unwrap();
        if path == "/stall" {
            assert!(
                tokio::time::timeout(Duration::from_millis(50), writer.write(input))
                    .await
                    .is_err()
            );
        } else {
            assert!(writer.write(input).await.is_err());
        }
        assert_eq!(budget.buffered_bytes(), 0);
        assert!(writer.commit().await.is_err());
        assert!(!destination.exists());
    }
}
