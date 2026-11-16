//! A non-persistent sink illustrating physical output vs session publication.
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering as AtomicOrdering},
};

use open_arrow_sink_core::{
    DataWriter, Error, ParallelSink, RecordBatch, Result, SchemaRef, SessionCommitter, Sink,
    SinkCapability, WriteMode, WriteOptions, WriteSession, WriteSummary, Writer, async_trait,
    validation,
};

/// The request is a per-session counter, not a durable destination. Use a fresh
/// counter for each session; commit exposes its row count and abort leaves it alone.
pub struct CountingSink;

#[async_trait]
impl Sink for CountingSink {
    type Request = Arc<AtomicU64>;

    fn capability(&self) -> SinkCapability {
        SinkCapability::default().with_supported_modes(&[WriteMode::Create])
    }

    async fn open(
        &self,
        request: Self::Request,
        schema: SchemaRef,
        options: WriteOptions,
    ) -> Result<Box<dyn Writer>> {
        self.open_writers(request, schema, options, std::num::NonZeroUsize::MIN)
            .await?
            .into_writer()
            .await
    }
}

#[async_trait]
impl ParallelSink for CountingSink {
    type Output = u64;

    async fn open_writers(
        &self,
        request: Self::Request,
        schema: SchemaRef,
        options: WriteOptions,
        count: std::num::NonZeroUsize,
    ) -> Result<WriteSession<Self::Output>> {
        validation::validate_options(&schema, &options, self.capability())?;
        // This demonstration has no data files, so a target size cannot be honored.
        if options.target_file_size.is_some() {
            return Err(Error::Unsupported("counting sink has no data files".into()));
        }
        Ok(WriteSession {
            writers: (0..count.get())
                .map(|_| {
                    Box::new(CountWriter {
                        schema: schema.clone(),
                        rows: 0,
                        state: open_arrow_sink_core::Poison::default(),
                    }) as Box<dyn DataWriter<Output = u64>>
                })
                .collect(),
            committer: Box::new(CountCommitter(request)),
        })
    }
}

struct CountWriter {
    schema: SchemaRef,
    rows: u64,
    state: open_arrow_sink_core::Poison,
}

#[async_trait]
impl DataWriter for CountWriter {
    type Output = u64;
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    async fn write(&mut self, batch: RecordBatch) -> Result<()> {
        // Set before validation / any awaited work. Errors and cancellation must
        // never leave the writer reusable. This in-memory example has no I/O.
        self.state.begin()?;
        validation::validate_batch(&self.schema, &batch)?;
        self.rows = self
            .rows
            .checked_add(batch.num_rows() as u64)
            .ok_or_else(|| Error::Unsupported("row count overflow".into()))?;
        self.state.complete();
        Ok(())
    }

    async fn finish(&mut self) -> Result<u64> {
        self.state.begin()?;
        Ok(self.rows)
    }

    async fn abort(self: Box<Self>) -> Result<()> {
        // No external output exists, including after finish.
        Ok(())
    }
}

struct CountCommitter(Arc<AtomicU64>);

#[async_trait]
impl SessionCommitter<u64> for CountCommitter {
    async fn commit(self: Box<Self>, outputs: Vec<u64>) -> Result<WriteSummary> {
        let rows_written = outputs.into_iter().try_fold(0_u64, |total, rows| {
            total
                .checked_add(rows)
                .ok_or_else(|| Error::Unsupported("row count overflow".into()))
        })?;
        self.0
            .compare_exchange(
                0,
                rows_written,
                AtomicOrdering::SeqCst,
                AtomicOrdering::SeqCst,
            )
            .map_err(|_| Error::Unsupported("use a fresh destination counter".into()))?;
        Ok(WriteSummary { rows_written })
    }
    async fn abort(self: Box<Self>) -> Result<()> {
        Ok(())
    }
}
