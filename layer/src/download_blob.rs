use std::{
    collections::{HashSet, VecDeque},
    num::NonZeroUsize,
    sync::{Arc, Mutex},
};

use arrow_array::{
    Array, BooleanArray, LargeStringArray, StringArray, StringViewArray,
    builder::LargeBinaryBuilder,
};
use arrow_schema::{DataType, Schema};
use futures::{FutureExt, StreamExt, future::BoxFuture, stream::FuturesUnordered};
use open_arrow_sink_core::{
    BLOB_METADATA_KEY, BlobPlacement, BlobWriter, Layer, RecordBatch, Result, SchemaRef, Sink,
    SinkCapability, WriteOptions, WriteSummary, Writer, async_trait, validation,
};

use crate::{
    DownloadPool,
    error::{self, backend},
    validation::validate_download_config,
};

/// Treatment of a source URL that cannot be downloaded.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub enum DownloadFailurePolicy {
    /// Fail the session (default).
    #[default]
    Fail,
    /// Drop the entire row if any selected URL fails; retain order of other rows.
    SkipRow,
    /// Replace each failed blob with null. Selected output fields become nullable.
    Null,
}

/// Observations across sessions using a download metrics handle.
/// A source can fail after transferring bytes. Destination failures and input
/// nulls do not count as source-download failures.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct DownloadSnapshot {
    pub blobs_failed: u64,
    /// Unique failed rows within each input batch, even with multiple failed URLs.
    pub rows_failed_blobs: u64,
    /// Rows omitted from successfully forwarded batches by SkipRow.
    pub rows_skipped: u64,
}

#[derive(Clone, Debug, Default)]
pub struct DownloadMetrics(Arc<Mutex<DownloadSnapshot>>);
impl DownloadMetrics {
    pub fn snapshot(&self) -> DownloadSnapshot {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }
    fn failure(&self, new_row: bool) {
        let mut snapshot = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        snapshot.blobs_failed += 1;
        snapshot.rows_failed_blobs += u64::from(new_row);
    }
    fn skipped(&self, rows: usize) {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .rows_skipped += rows as u64;
    }
}

/// Stream selected URL columns into native blobs and attach their descriptors.
/// Requires a sink supporting `Writer::create_blob` and `write_with_blobs`.
/// By default input boundaries, order, nulls, and unselected columns are preserved.
/// Optional failure policies discard only source-download failures; destination
/// write, finish, and cleanup failures remain terminal.
#[derive(Clone, Debug)]
pub struct DownloadBlobLayer {
    columns: HashSet<String>,
    download_pool: Option<Arc<DownloadPool>>,
    failure_policy: DownloadFailurePolicy,
    metrics: DownloadMetrics,
    batch_window: NonZeroUsize,
}
impl DownloadBlobLayer {
    pub fn new(columns: HashSet<String>) -> Self {
        Self {
            columns,
            download_pool: None,
            failure_policy: DownloadFailurePolicy::Fail,
            metrics: DownloadMetrics::default(),
            batch_window: NonZeroUsize::new(1).unwrap(),
        }
    }
    /// Allow downloads from this many input batches to overlap. Defaults to one.
    /// With a larger window, write success means accepted into the bounded queue;
    /// downloads progress only while subsequent writes or commit are polled.
    /// Completed batches are forwarded in input order. Retained input arrays and
    /// completed blob handles are bounded by the window, not the transfer-byte pool.
    pub fn with_batch_window(mut self, batches: NonZeroUsize) -> Self {
        self.batch_window = batches;
        self
    }
    pub fn with_failure_policy(mut self, policy: DownloadFailurePolicy) -> Self {
        self.failure_policy = policy;
        self
    }
    pub fn metrics(&self) -> DownloadMetrics {
        self.metrics.clone()
    }
    pub fn with_download_pool(mut self, pool: Arc<DownloadPool>) -> Self {
        self.download_pool = Some(pool);
        self
    }
}

pub struct DownloadBlobSink<S> {
    inner: S,
    config: DownloadBlobLayer,
}
impl<S: Sink> Layer<S> for DownloadBlobLayer {
    type Layered = DownloadBlobSink<S>;
    fn layer(&self, inner: S) -> Self::Layered {
        DownloadBlobSink {
            inner,
            config: self.clone(),
        }
    }
}
#[async_trait]
impl<S: Sink> Sink for DownloadBlobSink<S> {
    type Request = S::Request;
    fn capability(&self) -> SinkCapability {
        // This writer consumes native streams internally but does not expose
        // create_blob/write_with_blobs to its caller.
        SinkCapability {
            supports_blob_streaming: false,
            ..self.inner.capability()
        }
    }
    async fn open(
        &self,
        request: Self::Request,
        schema: SchemaRef,
        options: WriteOptions,
    ) -> Result<Box<dyn Writer>> {
        let indices = validate_download_config(&schema, &self.config.columns)?;
        crate::validation::validate_download_support(
            !indices.is_empty(),
            self.inner.capability().supports_blob_streaming,
        )?;
        let mut fields = schema.fields().to_vec();
        for &column_index in &indices {
            let field = schema.field(column_index);
            let mut metadata = field.metadata().clone();
            metadata.remove("ARROW:extension:name");
            metadata.remove("ARROW:extension:metadata");
            metadata.insert(BLOB_METADATA_KEY.into(), "true".into());
            fields[column_index] = Arc::new(
                field
                    .clone()
                    .with_data_type(DataType::LargeBinary)
                    .with_nullable(
                        field.is_nullable()
                            || self.config.failure_policy == DownloadFailurePolicy::Null,
                    )
                    .with_metadata(metadata),
            );
        }
        let output_schema = Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone()));
        let pool = match &self.config.download_pool {
            Some(pool) => pool.clone(),
            None => DownloadPool::global()?,
        };
        let io_metrics = options.io_metrics.clone();
        let inner = self
            .inner
            .open(request, output_schema.clone(), options)
            .await?;
        Ok(Box::new(DownloadBlobWriter {
            inner,
            schema,
            output_schema,
            indices,
            pool,
            failed: false,
            io_metrics,
            failure_policy: self.config.failure_policy,
            metrics: self.config.metrics.clone(),
            batch_window: self.config.batch_window,
            pending: VecDeque::new(),
            active: FuturesUnordered::new(),
            next_batch: 0,
        }))
    }
}

struct DownloadBlobWriter {
    inner: Box<dyn Writer>,
    schema: SchemaRef,
    output_schema: SchemaRef,
    indices: Vec<usize>,
    pool: Arc<DownloadPool>,
    failed: bool,
    io_metrics: Option<open_arrow_sink_core::IoMetrics>,
    failure_policy: DownloadFailurePolicy,
    metrics: DownloadMetrics,
    batch_window: NonZeroUsize,
    pending: VecDeque<PendingBatch>,
    active: FuturesUnordered<BoxFuture<'static, (u64, Result<DownloadResult>)>>,
    next_batch: u64,
}
struct PendingBatch {
    id: u64,
    rows: usize,
    columns: Vec<arrow_array::ArrayRef>,
    jobs: VecDeque<(usize, usize, String)>,
    remaining: usize,
    blobs: Vec<BlobPlacement>,
    failed_rows: HashSet<usize>,
    failed_cells: HashSet<(usize, usize)>,
}
fn url_value(array: &dyn Array, row: usize) -> &str {
    match array.data_type() {
        DataType::Utf8 => array
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(row),
        DataType::LargeUtf8 => array
            .as_any()
            .downcast_ref::<LargeStringArray>()
            .unwrap()
            .value(row),
        DataType::Utf8View => array
            .as_any()
            .downcast_ref::<StringViewArray>()
            .unwrap()
            .value(row),
        _ => unreachable!("validated at open"),
    }
}
enum DownloadResult {
    Complete(BlobPlacement),
    SourceFailed {
        column: usize,
        row: usize,
        error: open_arrow_sink_core::Error,
        cleanup: Result<()>,
    },
}
async fn download(
    pool: Arc<DownloadPool>,
    url: String,
    mut native: Box<dyn BlobWriter>,
    name: String,
    column: usize,
    row: usize,
    io_metrics: Option<open_arrow_sink_core::IoMetrics>,
) -> Result<DownloadResult> {
    // Only source failures are recoverable. Never suppress destination failures
    // or restart an upload after bytes have been accepted by its writer.
    let mut stream = match pool.stream(&url).await {
        Ok(stream) => stream,
        Err(error) => {
            let cleanup = native.abort().await;
            return Ok(DownloadResult::SourceFailed {
                column,
                row,
                error: error::download_context(&name, row, error),
                cleanup,
            });
        }
    };
    while let Some(chunk) = stream.next().await {
        let chunk = match chunk {
            Ok(chunk) => chunk,
            Err(error) => {
                drop(stream);
                let cleanup = native.abort().await;
                return Ok(DownloadResult::SourceFailed {
                    column,
                    row,
                    error: error::download_context(&name, row, error),
                    cleanup,
                });
            }
        };
        if let Some(metrics) = &io_metrics {
            metrics.record_read(chunk.as_ref().len() as u64);
        }
        native
            .write(chunk)
            .await
            .map_err(|error| error::download_context(&name, row, error))?;
    }
    drop(stream);
    let blob = native
        .finish()
        .await
        .map_err(|error| error::download_context(&name, row, error))?;
    Ok(DownloadResult::Complete(BlobPlacement {
        column,
        row,
        blob,
    }))
}
impl DownloadBlobWriter {
    fn enqueue(&mut self, batch: RecordBatch) {
        let mut columns = batch.columns().to_vec();
        let mut jobs = VecDeque::new();
        for &column in &self.indices {
            let array = batch.column(column);
            let mut builder = LargeBinaryBuilder::new();
            for row in 0..batch.num_rows() {
                if array.is_null(row) {
                    builder.append_null();
                } else {
                    builder.append_value([]);
                    jobs.push_back((column, row, url_value(array.as_ref(), row).to_owned()));
                }
            }
            columns[column] = Arc::new(builder.finish());
        }
        let id = self.next_batch;
        self.next_batch += 1;
        self.pending.push_back(PendingBatch {
            id,
            rows: batch.num_rows(),
            columns,
            remaining: jobs.len(),
            jobs,
            blobs: Vec::new(),
            failed_rows: HashSet::new(),
            failed_cells: HashSet::new(),
        });
    }
    async fn schedule(&mut self) -> Result<()> {
        while self.active.len() < self.pool.concurrency() {
            let Some(batch) = self.pending.iter_mut().find(|batch| !batch.jobs.is_empty()) else {
                break;
            };
            let id = batch.id;
            let (column, row, url) = batch.jobs.pop_front().unwrap();
            let native = self.inner.create_blob().await?;
            let future = download(
                self.pool.clone(),
                url,
                native,
                self.schema.field(column).name().clone(),
                column,
                row,
                self.io_metrics.clone(),
            );
            self.active.push(async move { (id, future.await) }.boxed());
        }
        Ok(())
    }
    fn completed(&mut self, id: u64, result: Result<DownloadResult>) -> Result<()> {
        let batch = self
            .pending
            .iter_mut()
            .find(|batch| batch.id == id)
            .expect("an active download belongs to a queued batch");
        batch.remaining -= 1;
        match result? {
            DownloadResult::Complete(blob) => batch.blobs.push(blob),
            DownloadResult::SourceFailed {
                column,
                row,
                error,
                cleanup,
            } => {
                self.metrics.failure(batch.failed_rows.insert(row));
                cleanup?;
                batch.failed_cells.insert((column, row));
                if self.failure_policy == DownloadFailurePolicy::Fail {
                    return Err(error);
                }
            }
        }
        Ok(())
    }
    async fn forward_ready(&mut self) -> Result<()> {
        while self
            .pending
            .front()
            .is_some_and(|batch| batch.remaining == 0)
        {
            let PendingBatch {
                rows,
                mut columns,
                mut blobs,
                failed_rows,
                failed_cells,
                ..
            } = self.pending.pop_front().unwrap();
            if self.failure_policy == DownloadFailurePolicy::Null {
                for &column in &self.indices {
                    let values = columns[column]
                        .as_any()
                        .downcast_ref::<arrow_array::LargeBinaryArray>()
                        .unwrap();
                    let mut builder = LargeBinaryBuilder::new();
                    for row in 0..rows {
                        if values.is_null(row) || failed_cells.contains(&(column, row)) {
                            builder.append_null();
                        } else {
                            builder.append_value([]);
                        }
                    }
                    columns[column] = Arc::new(builder.finish());
                }
            }
            let mut output =
                RecordBatch::try_new(self.output_schema.clone(), columns).map_err(backend)?;
            if self.failure_policy == DownloadFailurePolicy::SkipRow && !failed_rows.is_empty() {
                let keep = BooleanArray::from(
                    (0..rows)
                        .map(|row| !failed_rows.contains(&row))
                        .collect::<Vec<_>>(),
                );
                output =
                    arrow_select::filter::filter_record_batch(&output, &keep).map_err(backend)?;
                let mut row_map = vec![None; rows];
                let mut next = 0;
                for (row, target) in row_map.iter_mut().enumerate() {
                    if !failed_rows.contains(&row) {
                        *target = Some(next);
                        next += 1;
                    }
                }
                blobs.retain_mut(|blob| match row_map[blob.row] {
                    Some(row) => {
                        blob.row = row;
                        true
                    }
                    None => false,
                });
            }
            self.inner.write_with_blobs(output, blobs).await?;
            if self.failure_policy == DownloadFailurePolicy::SkipRow {
                self.metrics.skipped(failed_rows.len());
            }
        }
        Ok(())
    }
    async fn progress(&mut self, wait: bool) -> Result<()> {
        self.schedule().await?;
        self.forward_ready().await?;
        let event = if wait {
            self.active.next().await
        } else {
            self.active.next().now_or_never().flatten()
        };
        if let Some((id, result)) = event {
            self.completed(id, result)?;
        }
        loop {
            self.schedule().await?;
            self.forward_ready().await?;
            match self.active.next().now_or_never().flatten() {
                Some((id, result)) => self.completed(id, result)?,
                None => break,
            }
        }
        Ok(())
    }
    async fn drain(&mut self) -> Result<()> {
        while !self.pending.is_empty() {
            self.progress(true).await?;
        }
        Ok(())
    }
}
#[async_trait]
impl Writer for DownloadBlobWriter {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }
    async fn write(&mut self, batch: RecordBatch) -> Result<()> {
        crate::validation::validate_download_writer(self.failed)?;
        // Cancellation and all errors are terminal, including queue backpressure.
        self.failed = true;
        validation::validate_batch(&self.schema, &batch)?;
        if self.indices.is_empty() {
            self.inner.write(batch).await?;
        } else {
            while self.pending.len() >= self.batch_window.get() {
                self.progress(true).await?;
            }
            self.enqueue(batch);
            self.progress(false).await?;
            if self.batch_window.get() == 1 {
                self.drain().await?;
            }
        }
        self.failed = false;
        Ok(())
    }
    async fn commit(mut self: Box<Self>) -> Result<WriteSummary> {
        if crate::validation::validate_download_writer(self.failed).is_err() {
            let _ = self.abort().await;
            return Err(error::download_aborted());
        }
        if let Err(error) = self.drain().await {
            let _ = self.abort().await;
            return Err(error);
        }
        self.inner.commit().await
    }
    async fn abort(mut self: Box<Self>) -> Result<()> {
        // Cancel every source/upload future before cleanup; nothing is detached.
        self.active.clear();
        self.pending.clear();
        self.inner.abort().await
    }
}
