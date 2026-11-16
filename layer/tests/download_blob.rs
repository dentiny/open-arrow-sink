use std::{
    num::NonZeroUsize,
    sync::{Arc, Mutex},
    time::Duration,
};

use arrow_array::cast::AsArray;
use arrow_array::{
    Array, ArrayRef, Int32Array, LargeBinaryArray, LargeStringArray, StringArray, StringViewArray,
};
use arrow_schema::{DataType, Field, Schema};
use open_arrow_sink_core::{
    BLOB_METADATA_KEY, BlobChunk, BlobHandle, BlobPlacement, BlobWriter, Ordering, RecordBatch,
    Result, SchemaRef, Sink, SinkCapability, SinkExt, WriteOptions, WriteSummary, Writer,
    async_trait, validation,
};
use open_arrow_sink_layer::{DownloadBlobLayer, DownloadPool};
use open_arrow_sink_test_support::http::{HttpStatus, Reply, Server, serve};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    task::JoinHandle,
};

// A 4 MiB buffer budget admits two of the common downloader's 2 MiB chunks.
fn download_pool() -> DownloadPool {
    DownloadPool::new(
        NonZeroUsize::new(2).unwrap(),
        NonZeroUsize::new(4 * 1024 * 1024).unwrap(),
    )
    .unwrap()
}
fn download_layer<'a>(columns: impl IntoIterator<Item = &'a str>) -> DownloadBlobLayer {
    layer_with_pool(columns, Arc::new(download_pool()))
}
fn layer_with_pool<'a>(
    columns: impl IntoIterator<Item = &'a str>,
    pool: Arc<DownloadPool>,
) -> DownloadBlobLayer {
    DownloadBlobLayer::new(columns.into_iter().map(str::to_owned).collect())
        .with_download_pool(pool)
}

#[derive(Default)]
struct State {
    schema: Option<SchemaRef>,
    batches: Vec<RecordBatch>,
    committed: bool,
    aborted: bool,
    fail_upload: bool,
    fail_blob_abort: bool,
    blobs_aborted: usize,
}
#[derive(Clone, Default)]
struct Capture(Arc<Mutex<State>>);
impl Capture {
    async fn writer(&self, layer: DownloadBlobLayer, schema: SchemaRef) -> Result<Box<dyn Writer>> {
        self.clone()
            .layer(layer)
            .open((), schema, WriteOptions::default())
            .await
    }
    fn binary(&self, column: usize) -> Vec<Option<Vec<u8>>> {
        self.0
            .lock()
            .unwrap()
            .batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column(column)
                    .as_binary::<i64>()
                    .iter()
                    .map(|value| value.map(Vec::from))
            })
            .collect()
    }
}
struct CaptureWriter {
    state: Capture,
    schema: SchemaRef,
}
#[async_trait]
impl Sink for Capture {
    type Request = ();
    fn capability(&self) -> SinkCapability {
        SinkCapability::default()
            .with_supported_modes(&[open_arrow_sink_core::WriteMode::Create])
            .with_ordering(Ordering::PreserveInput)
            .with_supports_blob_handle(true)
            .with_supports_blob_streaming(true)
    }
    async fn open(
        &self,
        _: (),
        schema: SchemaRef,
        options: WriteOptions,
    ) -> Result<Box<dyn Writer>> {
        validation::validate_options(&schema, &options, self.capability())?;
        self.0.lock().unwrap().schema = Some(schema.clone());
        Ok(Box::new(CaptureWriter {
            state: self.clone(),
            schema,
        }))
    }
}
#[async_trait]
impl Writer for CaptureWriter {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }
    async fn create_blob(&mut self) -> Result<Box<dyn BlobWriter>> {
        Ok(Box::new(CaptureBlob {
            bytes: Vec::new(),
            state: self.state.clone(),
        }))
    }
    async fn write_with_blobs(
        &mut self,
        batch: RecordBatch,
        blobs: Vec<BlobPlacement>,
    ) -> Result<()> {
        let mut columns = batch.columns().to_vec();
        for (column, array) in batch.columns().iter().enumerate() {
            if !blobs.iter().any(|blob| blob.column == column) {
                continue;
            }
            let array = array.as_binary::<i64>();
            let mut builder = arrow_array::builder::LargeBinaryBuilder::new();
            for row in 0..array.len() {
                if let Some(blob) = blobs
                    .iter()
                    .find(|blob| blob.column == column && blob.row == row)
                {
                    builder.append_value(blob.blob.downcast_ref::<Vec<u8>>().unwrap());
                } else if array.is_null(row) {
                    builder.append_null();
                } else {
                    builder.append_value(array.value(row));
                }
            }
            columns[column] = Arc::new(builder.finish());
        }
        self.write(RecordBatch::try_new(batch.schema(), columns).unwrap())
            .await
    }
    async fn write(&mut self, batch: RecordBatch) -> Result<()> {
        validation::validate_batch(&self.schema, &batch)?;
        self.state.0.lock().unwrap().batches.push(batch);
        Ok(())
    }
    async fn commit(self: Box<Self>) -> Result<WriteSummary> {
        let mut state = self.state.0.lock().unwrap();
        state.committed = true;
        Ok(WriteSummary {
            rows_written: state.batches.iter().map(|b| b.num_rows() as u64).sum(),
        })
    }
    async fn abort(self: Box<Self>) -> Result<()> {
        self.state.0.lock().unwrap().aborted = true;
        Ok(())
    }
}

struct CaptureBlob {
    bytes: Vec<u8>,
    state: Capture,
}
#[async_trait]
impl BlobWriter for CaptureBlob {
    async fn write(&mut self, chunk: BlobChunk) -> Result<()> {
        if self.state.0.lock().unwrap().fail_upload {
            return Err(open_arrow_sink_core::Error::Backend(Box::new(
                std::io::Error::other("destination failed"),
            )));
        }
        self.bytes.extend_from_slice(chunk.as_ref());
        Ok(())
    }
    async fn finish(self: Box<Self>) -> Result<BlobHandle> {
        Ok(BlobHandle::new(self.bytes))
    }
    async fn abort(self: Box<Self>) -> Result<()> {
        let mut state = self.state.0.lock().unwrap();
        state.blobs_aborted += 1;
        if state.fail_blob_abort {
            return Err(open_arrow_sink_core::Error::Backend(Box::new(
                std::io::Error::other("abort failed"),
            )));
        }
        Ok(())
    }
}

// Deliberately malformed HTTP cannot be represented by an axum response.
struct RawServer {
    url: String,
    task: JoinHandle<()>,
}
impl Drop for RawServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}
async fn fault_server(response: &[u8]) -> RawServer {
    let response = response.to_vec();
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
    RawServer { url, task }
}
fn batch(urls: Vec<Option<&str>>) -> RecordBatch {
    RecordBatch::try_from_iter([("url", Arc::new(StringArray::from(urls)) as ArrayRef)]).unwrap()
}

#[tokio::test]
async fn downloads_all_string_types_preserving_nulls_metadata_and_row_order() {
    let server = serve(|_| Reply::bytes(b"\x00\xffx".as_slice())).await;
    let urls = vec![Some(server.url.as_str()), None, Some(server.url.as_str())];
    let batch = RecordBatch::try_from_iter([
        (
            "utf8",
            Arc::new(StringArray::from(urls.clone())) as ArrayRef,
        ),
        (
            "large",
            Arc::new(LargeStringArray::from(urls.clone())) as ArrayRef,
        ),
        ("view", Arc::new(StringViewArray::from(urls)) as ArrayRef),
        ("id", Arc::new(Int32Array::from(vec![7, 2, 9])) as ArrayRef),
    ])
    .unwrap();
    let mut fields = batch.schema().fields().to_vec();
    fields[0] = Arc::new(
        fields[0]
            .as_ref()
            .clone()
            .with_metadata([("description".into(), "photo".into())].into()),
    );
    let schema = Arc::new(Schema::new_with_metadata(
        fields,
        [("source".into(), "test".into())].into(),
    ));
    let batch = RecordBatch::try_new(schema.clone(), batch.columns().to_vec()).unwrap();
    let capture = Capture::default();
    let mut writer = capture
        .writer(download_layer(["utf8", "large", "view"]), schema.clone())
        .await
        .unwrap();
    assert_eq!(writer.schema(), &schema);
    writer.write(RecordBatch::new_empty(schema)).await.unwrap();
    writer.write(batch).await.unwrap();
    assert_eq!(writer.commit().await.unwrap().rows_written, 3);
    let state = capture.0.lock().unwrap();
    let schema = state.schema.clone().unwrap();
    let batches = state.batches.clone();
    drop(state);
    assert_eq!(
        batches
            .iter()
            .map(RecordBatch::num_rows)
            .collect::<Vec<_>>(),
        [0, 3]
    );
    assert_eq!(schema.metadata()["source"], "test");
    assert_eq!(schema.field(0).metadata()["description"], "photo");
    for i in 0..3 {
        assert_eq!(schema.field(i).data_type(), &DataType::LargeBinary);
        assert_eq!(schema.field(i).metadata()[BLOB_METADATA_KEY], "true");
        let values = capture.binary(i);
        assert_eq!(
            values,
            vec![Some(vec![0, 255, b'x']), None, Some(vec![0, 255, b'x'])]
        );
    }
    let ids = batches
        .iter()
        .flat_map(|b| {
            b.column(3)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values()
                .to_vec()
        })
        .collect::<Vec<_>>();
    assert_eq!(ids, [7, 2, 9]);
}

#[tokio::test]
async fn empty_payload_is_distinct_from_null() {
    let server = serve(|_| Reply::bytes(b"".as_slice())).await;
    let batch = batch(vec![Some(&server.url), None]);
    let capture = Capture::default();
    let mut writer = capture
        .writer(download_layer(["url"]), batch.schema())
        .await
        .unwrap();
    writer.write(batch).await.unwrap();
    writer.commit().await.unwrap();
    assert_eq!(capture.binary(0), vec![Some(vec![]), None]);
}

#[rstest::rstest]
#[case::not_found(b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")]
#[case::truncated_body(b"HTTP/1.1 200 OK\r\nContent-Length: 4\r\nConnection: close\r\n\r\nabc")]
#[case::truncated_chunk(
    b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n4\r\nabc"
)]
#[tokio::test]
async fn failures_poison_session_and_commit_returns_without_aborting(#[case] response: &[u8]) {
    let server = fault_server(response).await;
    let batch = batch(vec![None, Some(&server.url)]);
    let capture = Capture::default();
    let mut writer = capture
        .writer(download_layer(["url"]), batch.schema())
        .await
        .unwrap();
    writer.write(batch.slice(0, 1)).await.unwrap();
    assert!(writer.write(batch.clone()).await.is_err());
    assert!(writer.write(batch.slice(0, 1)).await.is_err());
    assert!(writer.commit().await.is_err());
    let state = capture.0.lock().unwrap();
    assert_eq!(state.batches.len(), 1);
    assert!(!state.aborted);
    assert!(!state.committed);
}

#[tokio::test]
async fn rejects_bad_configuration_before_open_and_invalid_urls_before_write() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("url", DataType::Utf8, true),
        Field::new("id", DataType::Int32, false),
    ]));
    for columns in [vec!["missing"], vec!["id"]] {
        let capture = Capture::default();
        assert!(
            capture
                .writer(download_layer(columns), schema.clone())
                .await
                .is_err()
        );
        assert!(capture.0.lock().unwrap().schema.is_none());
    }
    let ambiguous = Arc::new(Schema::new(vec![
        Field::new("url", DataType::Utf8, true),
        Field::new("url", DataType::Utf8, true),
    ]));
    let capture = Capture::default();
    assert!(
        capture
            .writer(download_layer(["url"]), ambiguous)
            .await
            .is_err()
    );
    assert!(capture.0.lock().unwrap().schema.is_none());
    for url in ["file:///tmp/blob", "not a url"] {
        let capture = Capture::default();
        let batch = batch(vec![Some(url)]);
        let mut writer = capture
            .writer(download_layer(["url"]), batch.schema())
            .await
            .unwrap();
        assert!(writer.write(batch).await.is_err());
        writer.abort().await.unwrap();
        assert!(capture.0.lock().unwrap().batches.is_empty());
    }
}

// Send headers immediately, then hold each response body until the test releases
// it. This checks that pool permits cover bodies as well as request dispatch.
async fn gated_server() -> (
    Server,
    tokio::sync::mpsc::UnboundedReceiver<tokio::sync::oneshot::Sender<()>>,
) {
    let (events, received) = tokio::sync::mpsc::unbounded_channel();
    let server = serve(move |uri| {
        let (release, ready) = tokio::sync::oneshot::channel();
        let _ = events.send(release);
        Reply::bytes(uri.path_and_query().unwrap().as_str().as_bytes().to_vec()).gated(ready)
    })
    .await;
    (server, received)
}

async fn next_request(
    events: &mut tokio::sync::mpsc::UnboundedReceiver<tokio::sync::oneshot::Sender<()>>,
) -> tokio::sync::oneshot::Sender<()> {
    tokio::time::timeout(Duration::from_secs(5), events.recv())
        .await
        .expect("download should start")
        .expect("server should be running")
}

#[tokio::test]
async fn shared_pool_bounds_active_bodies_and_preserves_order() {
    let (server, mut events) = gated_server().await;
    let pool = Arc::new(download_pool());
    let captures = [Capture::default(), Capture::default()];
    let mut writes = tokio::task::JoinSet::new();
    for (capture, paths) in captures.iter().zip([["/a", "/b"], ["/c", "/d"]]) {
        let urls = paths.map(|path| format!("{}{path}", server.url));
        let batch = batch(urls.iter().map(|url| Some(url.as_str())).collect());
        let mut writer = capture
            .writer(layer_with_pool(["url"], pool.clone()), batch.schema())
            .await
            .unwrap();
        writes.spawn(async move {
            writer.write(batch).await.unwrap();
            writer.commit().await.unwrap();
        });
    }
    let first = next_request(&mut events).await;
    let second = next_request(&mut events).await;
    assert!(
        tokio::time::timeout(Duration::from_millis(20), events.recv())
            .await
            .is_err()
    );
    // Finish later requests before the first to exercise result reordering.
    second.send(()).unwrap();
    next_request(&mut events).await.send(()).unwrap();
    next_request(&mut events).await.send(()).unwrap();
    first.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        while let Some(result) = writes.join_next().await {
            result.unwrap();
        }
    })
    .await
    .unwrap();
    for (capture, expected) in captures.iter().zip([[b"/a", b"/b"], [b"/c", b"/d"]]) {
        let values = capture.binary(0);
        assert_eq!(values[0].as_deref().unwrap(), expected[0]);
        assert_eq!(values[1].as_deref().unwrap(), expected[1]);
        assert!(capture.0.lock().unwrap().committed);
    }
}

#[tokio::test]
async fn cancelling_download_releases_shared_pool_capacity() {
    let (server, mut events) = gated_server().await;
    let pool = Arc::new(download_pool());
    let input = batch(vec![Some(&server.url)]);
    let capture = Capture::default();
    let layer = layer_with_pool(["url"], pool);
    let mut writer = capture.writer(layer.clone(), input.schema()).await.unwrap();
    let mut write = Box::pin(writer.write(input.clone()));
    let held_body = tokio::select! {
        _ = &mut write => panic!("body has not been released"),
        request = next_request(&mut events) => request,
    };
    drop(write);
    assert!(writer.commit().await.is_err());
    assert!(!capture.0.lock().unwrap().aborted);

    assert_pool_has_capacity(layer, input, &mut events).await;
    drop(held_body);
}

#[tokio::test]
async fn downloads_columns_concurrently_with_single_row_batches() {
    let (server, mut events) = gated_server().await;
    let left = format!("{}/left", server.url);
    let right = format!("{}/right", server.url);
    let input = RecordBatch::try_from_iter([
        ("left", Arc::new(StringArray::from(vec![left])) as ArrayRef),
        (
            "right",
            Arc::new(StringArray::from(vec![right])) as ArrayRef,
        ),
    ])
    .unwrap();
    let capture = Capture::default();
    let mut writer = capture
        .writer(download_layer(["left", "right"]), input.schema())
        .await
        .unwrap();
    let write = tokio::spawn(async move {
        writer.write(input).await.unwrap();
        writer.commit().await.unwrap();
    });
    let first = next_request(&mut events).await;
    let second = next_request(&mut events).await;
    second.send(()).unwrap();
    first.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(5), write)
        .await
        .unwrap()
        .unwrap();
    for (index, expected) in [b"/left".as_slice(), b"/right"].into_iter().enumerate() {
        let values = capture.binary(index);
        assert_eq!(values[0].as_deref().unwrap(), expected);
    }
}

#[tokio::test]
async fn opendal_preserves_signed_queries_and_escaped_paths() {
    let (server, mut events) = gated_server().await;
    let paths = [
        "/images//a%2Fb%20c?X-Amz-Signature=a%2Fb%2B&x=1+2&x=3",
        "/?token=a%2Fb",
        "/trailing/?download=1",
    ];
    let urls = paths.map(|path| format!("{}{path}#ignored-fragment", server.url));
    let input = batch(urls.iter().map(|url| Some(url.as_str())).collect());
    let capture = Capture::default();
    let mut writer = capture
        .writer(download_layer(["url"]), input.schema())
        .await
        .unwrap();
    let write = tokio::spawn(async move {
        writer.write(input).await.unwrap();
        writer.commit().await.unwrap();
    });
    for _ in paths {
        next_request(&mut events).await.send(()).unwrap();
    }
    tokio::time::timeout(Duration::from_secs(5), write)
        .await
        .unwrap()
        .unwrap();
    let values = capture.binary(0);
    for (row, path) in paths.into_iter().enumerate() {
        assert_eq!(values[row].as_deref().unwrap(), path.as_bytes());
    }
}

#[tokio::test]
async fn opendal_timeout_on_body_releases_pool_capacity() {
    use open_arrow_sink_storage::{StorageConfig, TimeoutConfig};

    let (server, mut events) = gated_server().await;
    let config = StorageConfig {
        timeout: Some(TimeoutConfig {
            io_timeout: Duration::from_millis(100),
            ..TimeoutConfig::default()
        }),
        ..StorageConfig::default()
    };
    let pool = Arc::new(download_pool().with_http_config(config).unwrap());
    let layer = layer_with_pool(["url"], pool);
    let input = batch(vec![Some(&server.url)]);
    let capture = Capture::default();
    let mut writer = capture.writer(layer.clone(), input.schema()).await.unwrap();
    let mut write = Box::pin(writer.write(input.clone()));
    let held_body = tokio::select! {
        _ = &mut write => panic!("response body should still be pending"),
        request = next_request(&mut events) => request,
    };
    let error = tokio::time::timeout(Duration::from_secs(5), write)
        .await
        .unwrap()
        .unwrap_err();
    assert!(error.to_string().contains("timeout"), "{error}");
    assert!(writer.commit().await.is_err());
    assert!(!capture.0.lock().unwrap().aborted);

    assert_pool_has_capacity(layer, input, &mut events).await;
    drop(held_body);
}

#[rstest::rstest]
#[case::disabled(false)]
#[case::enabled(true)]
#[tokio::test]
async fn opendal_retries_transient_errors_only_when_configured(#[case] retry: bool) {
    use open_arrow_sink_storage::{RetryConfig, StorageConfig, TimeoutConfig};

    let first = std::sync::atomic::AtomicBool::new(true);
    let server = serve(move |uri| {
        assert_eq!(uri.path_and_query().unwrap().as_str(), "/blob?token=a%2Fb");
        if first.swap(false, std::sync::atomic::Ordering::SeqCst) {
            Reply::bytes(b"".as_slice()).status(HttpStatus::SERVICE_UNAVAILABLE)
        } else {
            Reply::bytes(b"ok".as_slice())
        }
    })
    .await;
    let url = format!("{}/blob?token=a%2Fb", server.url);
    let mut pool = download_pool();
    if retry {
        let config = StorageConfig {
            retry: Some(RetryConfig {
                max_retries: 1,
                min_delay: Duration::from_millis(1),
                max_delay: Duration::from_millis(1),
                ..RetryConfig::default()
            }),
            timeout: Some(TimeoutConfig {
                io_timeout: Duration::from_secs(1),
                ..TimeoutConfig::default()
            }),
        };
        pool = pool.with_http_config(config).unwrap();
    }
    let capture = Capture::default();
    let input = batch(vec![Some(&url)]);
    let mut writer = capture
        .writer(layer_with_pool(["url"], Arc::new(pool)), input.schema())
        .await
        .unwrap();
    let result = tokio::time::timeout(Duration::from_secs(5), writer.write(input))
        .await
        .unwrap();
    assert_eq!(result.is_ok(), retry, "{result:?}");
    assert_eq!(server.requests(), if retry { 2 } else { 1 });
    if retry {
        writer.commit().await.unwrap();
        let values = capture.binary(0);
        assert_eq!(values[0].as_deref().unwrap(), b"ok");
    } else {
        assert!(writer.commit().await.is_err());
        assert!(!capture.0.lock().unwrap().aborted);
    }
}

#[tokio::test]
async fn rejects_missing_streaming_capability_before_open() {
    struct UnsupportedSink;
    #[async_trait]
    impl Sink for UnsupportedSink {
        type Request = ();
        fn capability(&self) -> SinkCapability {
            Capture::default()
                .capability()
                .with_supports_blob_streaming(false)
        }
        async fn open(&self, _: (), _: SchemaRef, _: WriteOptions) -> Result<Box<dyn Writer>> {
            panic!("unsupported sink must not be opened");
        }
    }
    let input = batch(vec![Some("https://example.invalid/blob")]);
    let result = UnsupportedSink
        .layer(download_layer(["url"]))
        .open((), input.schema(), WriteOptions::default())
        .await;
    assert!(matches!(
        result,
        Err(open_arrow_sink_core::Error::Unsupported(_))
    ));
}

#[tokio::test]
async fn download_layer_consumes_streaming_capability_instead_of_forwarding_it() {
    let capture = Capture::default();
    let sink = capture.clone().layer(download_layer(["url"]));
    assert!(sink.capability().supports_blob_handle);
    assert!(!sink.capability().supports_blob_streaming);
    let input = batch(vec![Some("https://example.invalid/blob")]);
    let result = sink
        .layer(download_layer(["url"]))
        .open((), input.schema(), WriteOptions::default())
        .await;
    assert!(matches!(
        result,
        Err(open_arrow_sink_core::Error::Unsupported(_))
    ));
    assert!(capture.0.lock().unwrap().schema.is_none());
}

#[tokio::test]
async fn downloaded_bytes_are_visible_before_commit_and_survive_abort() {
    let server = serve(|_| Reply::bytes(b"abc".as_slice())).await;
    let input = batch(vec![Some(&server.url)]);
    let layer = open_arrow_sink_layer::MetricsLayer::default();
    let metrics = layer.metrics();
    let sink = Capture::default()
        .layer(download_layer(["url"]))
        .layer(layer);
    let mut writer = sink
        .open((), input.schema(), WriteOptions::default())
        .await
        .unwrap();
    writer.write(input).await.unwrap();
    assert_eq!(metrics.snapshot().bytes_read, 3);
    writer.abort().await.unwrap();
    assert_eq!(metrics.snapshot().bytes_read, 3);
    assert_eq!(metrics.snapshot().rows_committed, 0);
}

#[rstest::rstest]
#[case::failed_download(false)]
#[case::cancelled_download(true)]
#[tokio::test]
async fn partial_download_bytes_survive_failure_or_cancellation(#[case] cancel: bool) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let (release, released) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut request = [0; 4096];
        let _ = socket.read(&mut request).await.unwrap();
        socket
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 4\r\nConnection: close\r\n\r\nabc")
            .await
            .unwrap();
        // Leave the body incomplete until the first chunk has been consumed.
        let _ = released.await;
    });
    let server = RawServer { url, task };
    let input = batch(vec![Some(&server.url)]);
    let layer = open_arrow_sink_layer::MetricsLayer::default();
    let metrics = layer.metrics();
    let sink = Capture::default()
        .layer(download_layer(["url"]))
        .layer(layer);
    let mut writer = sink
        .open((), input.schema(), WriteOptions::default())
        .await
        .unwrap();
    let mut write = Box::pin(writer.write(input));
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            tokio::select! {
                result = &mut write => panic!("body should still be incomplete: {result:?}"),
                _ = tokio::time::sleep(Duration::from_millis(1)) => {
                    if metrics.snapshot().bytes_read == 3 { break; }
                }
            }
        }
    })
    .await
    .unwrap();
    release.send(()).unwrap();
    if cancel {
        drop(write);
    } else {
        assert!(
            tokio::time::timeout(Duration::from_secs(5), write)
                .await
                .unwrap()
                .is_err()
        );
    }
    assert!(writer.commit().await.is_err());
    assert_eq!(metrics.snapshot().bytes_read, 3);
    assert_eq!(metrics.snapshot().rows_committed, 0);
}

#[rstest::rstest]
#[case::skip(open_arrow_sink_layer::DownloadFailurePolicy::SkipRow)]
#[case::null(open_arrow_sink_layer::DownloadFailurePolicy::Null)]
#[tokio::test]
async fn optional_failure_policy_counts_cells_and_unique_rows_and_keeps_session_usable(
    #[case] policy: open_arrow_sink_layer::DownloadFailurePolicy,
) {
    use open_arrow_sink_layer::DownloadFailurePolicy;
    let good = serve(|_| Reply::bytes(b"ok".as_slice())).await;
    let bad = serve(|_| Reply::bytes(b"".as_slice()).status(HttpStatus::NOT_FOUND)).await;
    let input = RecordBatch::try_from_iter([
        (
            "left",
            Arc::new(StringArray::from(vec![
                good.url.as_str(),
                bad.url.as_str(),
                bad.url.as_str(),
            ])) as ArrayRef,
        ),
        (
            "right",
            Arc::new(StringArray::from(vec![
                good.url.as_str(),
                bad.url.as_str(),
                good.url.as_str(),
            ])) as ArrayRef,
        ),
        (
            "id",
            Arc::new(Int32Array::from(vec![10, 11, 12])) as ArrayRef,
        ),
    ])
    .unwrap();
    let capture = Capture::default();
    let layer = download_layer(["left", "right"]).with_failure_policy(policy);
    let metrics = layer.metrics();
    let mut writer = capture
        .clone()
        .layer(layer)
        .open((), input.schema(), WriteOptions::default())
        .await
        .unwrap();
    writer.write(input.clone()).await.unwrap();
    writer.write(input.slice(0, 1)).await.unwrap();
    let summary = writer.commit().await.unwrap();
    assert_eq!(
        summary.rows_written,
        if policy == DownloadFailurePolicy::SkipRow {
            2
        } else {
            4
        }
    );
    let metrics = metrics.snapshot();
    assert_eq!(metrics.blobs_failed, 3);
    assert_eq!(metrics.rows_failed_blobs, 2);
    assert_eq!(
        metrics.rows_skipped,
        if policy == DownloadFailurePolicy::SkipRow {
            2
        } else {
            0
        }
    );
    let state = capture.0.lock().unwrap();
    assert_eq!(state.blobs_aborted, 3);
    let first = &state.batches[0];
    let ids = first
        .column(2)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    let left = first
        .column(0)
        .as_any()
        .downcast_ref::<LargeBinaryArray>()
        .unwrap();
    let right = first
        .column(1)
        .as_any()
        .downcast_ref::<LargeBinaryArray>()
        .unwrap();
    assert_eq!(left.value(0), b"ok");
    assert_eq!(right.value(0), b"ok");
    if policy == DownloadFailurePolicy::SkipRow {
        assert_eq!(ids.values().as_ref(), &[10]);
    } else {
        assert_eq!(ids.values().as_ref(), &[10, 11, 12]);
        assert!(first.schema().field(0).is_nullable());
        assert!(left.is_null(1) && left.is_null(2));
        assert!(right.is_null(1));
        assert_eq!(right.value(2), b"ok");
    }
}

#[rstest::rstest]
#[case::skip_upload(open_arrow_sink_layer::DownloadFailurePolicy::SkipRow, true)]
#[case::null_upload(open_arrow_sink_layer::DownloadFailurePolicy::Null, true)]
#[case::skip_cleanup(open_arrow_sink_layer::DownloadFailurePolicy::SkipRow, false)]
#[case::null_cleanup(open_arrow_sink_layer::DownloadFailurePolicy::Null, false)]
#[tokio::test]
async fn recoverable_policy_never_suppresses_destination_or_cleanup_errors(
    #[case] policy: open_arrow_sink_layer::DownloadFailurePolicy,
    #[case] upload: bool,
) {
    let source = serve(move |_| {
        if upload {
            Reply::bytes(b"ok".as_slice())
        } else {
            Reply::bytes(b"".as_slice()).status(HttpStatus::NOT_FOUND)
        }
    })
    .await;
    let input = batch(vec![Some(source.url.as_str())]);
    let capture = Capture::default();
    {
        let mut state = capture.0.lock().unwrap();
        state.fail_upload = upload;
        state.fail_blob_abort = !upload;
    }
    let layer = download_layer(["url"]).with_failure_policy(policy);
    let metrics = layer.metrics();
    let mut writer = capture
        .clone()
        .layer(layer)
        .open((), input.schema(), WriteOptions::default())
        .await
        .unwrap();
    assert!(writer.write(input).await.is_err());
    assert!(
        writer
            .write(batch(vec![Some(source.url.as_str())]))
            .await
            .is_err()
    );
    writer.abort().await.unwrap();
    let state = capture.0.lock().unwrap();
    assert!(state.aborted);
    assert!(!state.committed);
    assert!(state.batches.is_empty());
    assert_eq!(metrics.snapshot().blobs_failed, u64::from(!upload));
}

async fn assert_pool_has_capacity(
    layer: DownloadBlobLayer,
    input: RecordBatch,
    events: &mut tokio::sync::mpsc::UnboundedReceiver<tokio::sync::oneshot::Sender<()>>,
) {
    let mut next_writer = Capture::default()
        .writer(layer, input.schema())
        .await
        .unwrap();
    let mut write = Box::pin(next_writer.write(input));
    let body = tokio::select! {
        _ = &mut write => panic!("body has not been released"),
        request = next_request(events) => request,
    };
    body.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(5), write)
        .await
        .unwrap()
        .unwrap();
    next_writer.commit().await.unwrap();
}
