//! Run: cargo run -p open-arrow-sink-layer --example custom_layer
#[path = "support/counting_sink.rs"]
mod counting_sink;

use arrow_array::{ArrayRef, Int64Array};
use counting_sink::CountingSink;
use open_arrow_sink_core::{
    Error, Layer, Ordering, RecordBatch, Result, SchemaRef, Sink, SinkCapability, SinkExt,
    WriteOptions, WriteSummary, Writer, async_trait,
};
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering as AtomicOrdering},
};

/// Reject large batches without silently splitting or changing row semantics.
#[derive(Clone, Copy)]
struct MaxBatchRows(usize);
struct LimitedSink<S> {
    inner: S,
    max_rows: usize,
}

impl<S: Sink> Layer<S> for MaxBatchRows {
    type Layered = LimitedSink<S>;
    fn layer(&self, inner: S) -> Self::Layered {
        LimitedSink {
            inner,
            max_rows: self.0,
        }
    }
}

#[async_trait]
impl<S: Sink> Sink for LimitedSink<S> {
    type Request = S::Request;
    fn capability(&self) -> SinkCapability {
        self.inner.capability().with_supports_blob_streaming(false)
    }
    async fn open(
        &self,
        request: Self::Request,
        schema: SchemaRef,
        options: WriteOptions,
    ) -> Result<Box<dyn Writer>> {
        if self.max_rows == 0 {
            return Err(Error::Unsupported("max batch rows must be positive".into()));
        }
        Ok(Box::new(LimitedWriter {
            inner: self.inner.open(request, schema, options).await?,
            max_rows: self.max_rows,
            state: open_arrow_sink_core::Poison::default(),
        }))
    }
}

struct LimitedWriter {
    inner: Box<dyn Writer>,
    max_rows: usize,
    state: open_arrow_sink_core::Poison,
}
impl LimitedWriter {
    fn check_batch(&self, batch: &RecordBatch) -> Result<()> {
        open_arrow_sink_core::validation::validate_batch(self.schema(), batch)?;
        if batch.num_rows() > self.max_rows {
            return Err(Error::Unsupported("batch exceeds max rows".into()));
        }
        Ok(())
    }
}

#[async_trait]
impl Writer for LimitedWriter {
    fn schema(&self) -> &SchemaRef {
        self.inner.schema()
    }
    async fn write(&mut self, batch: RecordBatch) -> Result<()> {
        self.state.begin()?;
        self.check_batch(&batch)?;
        self.inner.write(batch).await?;
        self.state.complete();
        Ok(())
    }
    async fn commit(self: Box<Self>) -> Result<WriteSummary> {
        if let Err(error) = self.state.check() {
            let _ = self.inner.abort().await;
            return Err(error);
        }
        self.inner.commit().await
    }
    async fn abort(self: Box<Self>) -> Result<()> {
        self.inner.abort().await
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let batch =
        RecordBatch::try_from_iter([("id", Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef)])
            .map_err(Error::backend)?;
    let sink = CountingSink.layer(MaxBatchRows(3));
    let options = WriteOptions::default().with_ordering(Ordering::Unordered);
    let published = Arc::new(AtomicU64::new(0));
    let mut writer = sink
        .open(published.clone(), batch.schema(), options.clone())
        .await?;
    writer.write(batch.clone()).await?;
    assert_eq!(writer.commit().await?.rows_written, 3);
    assert_eq!(published.load(AtomicOrdering::SeqCst), 3);

    // Rejection must poison the wrapper: commit cannot publish earlier rows.
    let rejected = Arc::new(AtomicU64::new(0));
    let sink = CountingSink.layer(MaxBatchRows(2));
    let mut writer = sink.open(rejected.clone(), batch.schema(), options).await?;
    assert!(writer.write(batch.slice(0, 1)).await.is_ok());
    assert!(writer.write(batch).await.is_err());
    assert!(writer.commit().await.is_err());
    assert_eq!(rejected.load(AtomicOrdering::SeqCst), 0);
    println!("Accepted a valid batch; aborted the session with an oversized batch");
    Ok(())
}
