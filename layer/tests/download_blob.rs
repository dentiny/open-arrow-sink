#![cfg(feature = "download-blob")]

use std::{
    num::NonZeroUsize,
    sync::{Arc, Mutex},
    time::Duration,
};

use arrow_array::{
    Array, ArrayRef, Int32Array, LargeBinaryArray, LargeStringArray, StringArray, StringViewArray,
};
use arrow_schema::{DataType, Field, Schema};
use open_arrow_sink_core::{
    BLOB_METADATA_KEY, Ordering, RecordBatch, Result, SchemaRef, Sink, SinkCapability, SinkExt,
    WriteOptions, WriteSummary, Writer, async_trait, validation,
};
use open_arrow_sink_layer::DownloadBlobLayer;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    task::JoinHandle,
};

#[derive(Default)]
struct State {
    schema: Option<SchemaRef>,
    batches: Vec<RecordBatch>,
    committed: bool,
    aborted: bool,
}
#[derive(Clone, Default)]
struct Capture(Arc<Mutex<State>>);
struct CaptureWriter {
    state: Capture,
    schema: SchemaRef,
}
#[async_trait]
impl Sink for Capture {
    type Request = ();
    fn capability(&self) -> SinkCapability {
        SinkCapability {
            ordering: Ordering::PreserveInput,
            supported_partitions: vec![],
            supported_indexes: &[],
        }
    }
    async fn open(
        &self,
        _: (),
        schema: SchemaRef,
        options: WriteOptions,
    ) -> Result<Box<dyn Writer>> {
        validation::validate_options(&options, self.capability())?;
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

struct Server {
    url: String,
    task: JoinHandle<()>,
}
impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}
async fn server(response: &'static [u8], delay: Duration) -> Server {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let task = tokio::spawn(async move {
        loop {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = [0; 4096];
            let _ = socket.read(&mut request).await;
            tokio::time::sleep(delay).await;
            let _ = socket.write_all(response).await;
        }
    });
    Server { url, task }
}
fn batch(urls: Vec<Option<&str>>) -> RecordBatch {
    RecordBatch::try_from_iter([("url", Arc::new(StringArray::from(urls)) as ArrayRef)]).unwrap()
}

#[tokio::test]
async fn downloads_all_string_types_preserving_nulls_metadata_and_row_order() {
    let server = server(
        b"HTTP/1.1 200 OK\r\nContent-Length: 3\r\nConnection: close\r\n\r\n\x00\xffx",
        Duration::ZERO,
    )
    .await;
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
    let sink = capture.clone().layer(
        DownloadBlobLayer::new(
            ["utf8", "large", "view"]
                .map(str::to_owned)
                .into_iter()
                .collect(),
        )
        .with_rows_per_batch(NonZeroUsize::new(2).unwrap()),
    );
    let mut writer = sink
        .open((), schema.clone(), WriteOptions::default())
        .await
        .unwrap();
    assert_eq!(writer.schema(), &schema);
    writer.write(RecordBatch::new_empty(schema)).await.unwrap();
    writer.write(batch).await.unwrap();
    assert_eq!(writer.commit().await.unwrap().rows_written, 3);
    let state = capture.0.lock().unwrap();
    assert_eq!(
        state
            .batches
            .iter()
            .map(RecordBatch::num_rows)
            .collect::<Vec<_>>(),
        [2, 1]
    );
    let schema = state.schema.as_ref().unwrap();
    assert_eq!(schema.metadata()["source"], "test");
    assert_eq!(schema.field(0).metadata()["description"], "photo");
    for i in 0..3 {
        assert_eq!(schema.field(i).data_type(), &DataType::LargeBinary);
        assert_eq!(schema.field(i).metadata()[BLOB_METADATA_KEY], "true");
        let values = state
            .batches
            .iter()
            .flat_map(|b| {
                b.column(i)
                    .as_any()
                    .downcast_ref::<LargeBinaryArray>()
                    .unwrap()
                    .iter()
                    .map(|v| v.map(Vec::from))
            })
            .collect::<Vec<_>>();
        assert_eq!(
            values,
            vec![Some(vec![0, 255, b'x']), None, Some(vec![0, 255, b'x'])]
        );
    }
    let ids = state
        .batches
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
    let server = server(
        b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
        Duration::ZERO,
    )
    .await;
    let batch = batch(vec![Some(&server.url), None]);
    let capture = Capture::default();
    let mut writer = capture
        .clone()
        .layer(DownloadBlobLayer::new(
            ["url"].map(str::to_owned).into_iter().collect(),
        ))
        .open((), batch.schema(), WriteOptions::default())
        .await
        .unwrap();
    writer.write(batch).await.unwrap();
    writer.commit().await.unwrap();
    let state = capture.0.lock().unwrap();
    let array = state.batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<LargeBinaryArray>()
        .unwrap();
    assert!(!array.is_null(0));
    assert_eq!(array.value(0), b"");
    assert!(array.is_null(1));
}

#[tokio::test]
async fn failures_poison_session_and_commit_aborts_even_after_forwarded_rows() {
    for response in [
        b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".as_slice(),
        b"HTTP/1.1 200 OK\r\nContent-Length: 4\r\nConnection: close\r\n\r\nabcd",
        b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n4\r\nabcd\r\n0\r\n\r\n",
    ] {
        let server = server(response, Duration::ZERO).await;
        let batch = batch(vec![None, Some(&server.url)]);
        let capture = Capture::default();
        let mut writer = capture.clone().layer(DownloadBlobLayer::new(["url"].map(str::to_owned).into_iter().collect()).with_rows_per_batch(NonZeroUsize::new(1).unwrap()).with_max_blob_bytes(NonZeroUsize::new(3).unwrap())).open((), batch.schema(), WriteOptions::default()).await.unwrap();
        assert!(writer.write(batch.clone()).await.is_err());
        assert!(writer.write(batch.slice(0, 1)).await.is_err());
        assert!(writer.commit().await.is_err());
        let state = capture.0.lock().unwrap();
        assert_eq!(state.batches.len(), 1);
        assert!(state.aborted); assert!(!state.committed);
    }
}

#[tokio::test]
async fn cancellation_prevents_commit() {
    let server = server(
        b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n",
        Duration::from_secs(60),
    )
    .await;
    let capture = Capture::default();
    let batch = batch(vec![Some(&server.url)]);
    let mut writer = capture
        .clone()
        .layer(DownloadBlobLayer::new(
            ["url"].map(str::to_owned).into_iter().collect(),
        ))
        .open((), batch.schema(), WriteOptions::default())
        .await
        .unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(20), writer.write(batch))
            .await
            .is_err()
    );
    assert!(writer.commit().await.is_err());
    assert!(capture.0.lock().unwrap().aborted);
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
                .clone()
                .layer(DownloadBlobLayer::new(
                    columns.into_iter().map(str::to_owned).collect()
                ))
                .open((), schema.clone(), WriteOptions::default())
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
            .clone()
            .layer(DownloadBlobLayer::new(
                ["url".to_owned()].into_iter().collect()
            ))
            .open((), ambiguous, WriteOptions::default())
            .await
            .is_err()
    );
    assert!(capture.0.lock().unwrap().schema.is_none());
    for url in ["file:///tmp/blob", "not a url"] {
        let capture = Capture::default();
        let batch = batch(vec![Some(url)]);
        let mut writer = capture
            .clone()
            .layer(DownloadBlobLayer::new(
                ["url"].map(str::to_owned).into_iter().collect(),
            ))
            .open((), batch.schema(), WriteOptions::default())
            .await
            .unwrap();
        assert!(writer.write(batch).await.is_err());
        writer.abort().await.unwrap();
        assert!(capture.0.lock().unwrap().batches.is_empty());
    }
}
