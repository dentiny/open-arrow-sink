use std::num::NonZeroUsize;

use arrow_select::concat::concat_batches;
use open_arrow_sink_core::{
    Error, Layer, RecordBatch, Result, SchemaRef, Sink, SinkCapability, WriteOptions, WriteSummary,
    Writer, async_trait, validation,
};

/// Merge small batches and split large batches to a fixed row count.
/// Row order is preserved. Commit writes the final partial batch.
#[derive(Debug, Clone, Copy)]
pub struct RebatchLayer {
    rows: NonZeroUsize,
}

impl RebatchLayer {
    pub fn new(rows: NonZeroUsize) -> Self {
        Self { rows }
    }
}

pub struct RebatchSink<S> {
    inner: S,
    rows: NonZeroUsize,
}

impl<S: Sink> Layer<S> for RebatchLayer {
    type Layered = RebatchSink<S>;

    fn layer(&self, inner: S) -> Self::Layered {
        RebatchSink {
            inner,
            rows: self.rows,
        }
    }
}

#[async_trait]
impl<S: Sink> Sink for RebatchSink<S> {
    type Request = S::Request;

    fn capability(&self) -> SinkCapability {
        self.inner.capability()
    }

    async fn open(
        &self,
        request: Self::Request,
        schema: SchemaRef,
        options: WriteOptions,
    ) -> Result<Box<dyn Writer>> {
        validation::validate_options(&options, self.capability())?;
        let inner = self.inner.open(request, schema, options).await?;
        Ok(Box::new(RebatchWriter {
            inner,
            rows: self.rows.get(),
            pending: Vec::new(),
            pending_rows: 0,
        }))
    }
}

struct RebatchWriter {
    inner: Box<dyn Writer>,
    rows: usize,
    pending: Vec<RecordBatch>,
    pending_rows: usize,
}

impl RebatchWriter {
    async fn flush(&mut self) -> Result<()> {
        if self.pending_rows == 0 {
            return Ok(());
        }
        let batch = concat_batches(self.inner.schema(), &self.pending)
            .map_err(|error| Error::Backend(Box::new(error)))?;
        self.pending.clear();
        self.pending_rows = 0;
        self.inner.write(batch).await
    }
}

#[async_trait]
impl Writer for RebatchWriter {
    fn schema(&self) -> &SchemaRef {
        self.inner.schema()
    }

    async fn write(&mut self, batch: RecordBatch) -> Result<()> {
        validation::validate_batch(self.schema(), &batch)?;
        let mut offset = 0;
        while offset < batch.num_rows() {
            let length = (self.rows - self.pending_rows).min(batch.num_rows() - offset);
            self.pending.push(batch.slice(offset, length));
            self.pending_rows += length;
            offset += length;
            if self.pending_rows == self.rows {
                self.flush().await?;
            }
        }
        Ok(())
    }

    async fn commit(mut self: Box<Self>) -> Result<WriteSummary> {
        if let Err(error) = self.flush().await {
            let _ = self.inner.abort().await;
            return Err(error);
        }
        self.inner.commit().await
    }

    async fn abort(self: Box<Self>) -> Result<()> {
        // Pending rows are discarded without forwarding them to the backend.
        self.inner.abort().await
    }
}
