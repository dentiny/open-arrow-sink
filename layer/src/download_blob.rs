use std::{collections::HashSet, sync::Arc};

use arrow_array::{
    Array, BooleanArray, LargeBinaryArray, LargeStringArray, StringArray, StringViewArray,
};
use arrow_schema::{DataType, Schema};
use futures::{StreamExt, stream::FuturesUnordered};
use open_arrow_sink_core::{
    BLOB_METADATA_KEY, BlobPlacement, BlobWriter, Layer, Poison, RecordBatch, Result, SchemaRef,
    Sink, SinkCapability, WriteOptions, WriteSummary, Writer, async_trait, validation,
};

use crate::{
    DownloadMetrics, DownloadPool,
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
}
impl DownloadBlobLayer {
    pub fn new(columns: HashSet<String>) -> Self {
        Self {
            columns,
            download_pool: None,
            failure_policy: DownloadFailurePolicy::Fail,
            metrics: DownloadMetrics::default(),
        }
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
        self.inner.capability().with_supports_blob_streaming(false)
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
            None => Arc::new(DownloadPool::with_defaults()?),
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
            state: Poison::default(),
            io_metrics,
            failure_policy: self.config.failure_policy,
            metrics: self.config.metrics.clone(),
        }))
    }
}

struct DownloadBlobWriter {
    inner: Box<dyn Writer>,
    schema: SchemaRef,
    output_schema: SchemaRef,
    indices: Vec<usize>,
    pool: Arc<DownloadPool>,
    state: Poison,
    io_metrics: Option<open_arrow_sink_core::IoMetrics>,
    failure_policy: DownloadFailurePolicy,
    metrics: DownloadMetrics,
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
    // Drop the source stream before cleaning up a failed download. Destination
    // writes still return directly: only source errors can be recovered.
    let source_error = match pool.stream(&url).await {
        Ok(mut stream) => loop {
            match stream.next().await {
                Some(Ok(chunk)) => {
                    if let Some(metrics) = &io_metrics {
                        metrics.record_read(chunk.as_ref().len() as u64);
                    }
                    native
                        .write(chunk)
                        .await
                        .map_err(|error| error::download_context(&name, row, error))?;
                }
                Some(Err(error)) => break Some(error),
                None => break None,
            }
        },
        Err(error) => Some(error),
    };
    if let Some(error) = source_error {
        let cleanup = native.abort().await;
        return Ok(DownloadResult::SourceFailed {
            column,
            row,
            error: error::download_context(&name, row, error),
            cleanup,
        });
    }
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
#[async_trait]
impl Writer for DownloadBlobWriter {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }
    async fn write(&mut self, batch: RecordBatch) -> Result<()> {
        self.state.begin()?;
        validation::validate_batch(&self.schema, &batch)?;
        if self.indices.is_empty() {
            self.inner.write(batch).await?;
        } else {
            let mut jobs = self.indices.iter().flat_map(|&column| {
                let array = batch.column(column);
                (0..batch.num_rows())
                    .filter_map(move |row| array.is_valid(row).then_some((column, row)))
            });
            let mut active = FuturesUnordered::new();
            let mut blobs = Vec::new();
            let mut failed_rows = HashSet::new();
            let mut failed_cells = HashSet::new();
            loop {
                while active.len() < self.pool.concurrency() {
                    let Some((column, row)) = jobs.next() else {
                        break;
                    };
                    let native = self.inner.create_blob().await?;
                    let url = url_value(batch.column(column).as_ref(), row).to_owned();
                    active.push(download(
                        self.pool.clone(),
                        url,
                        native,
                        self.schema.field(column).name().clone(),
                        column,
                        row,
                        self.io_metrics.clone(),
                    ));
                }
                match active.next().await {
                    Some(result) => match result? {
                        DownloadResult::Complete(blob) => blobs.push(blob),
                        DownloadResult::SourceFailed {
                            column,
                            row,
                            error,
                            cleanup,
                        } => {
                            self.metrics.failure(failed_rows.insert(row));
                            cleanup?;
                            failed_cells.insert((column, row));
                            if self.failure_policy == DownloadFailurePolicy::Fail {
                                return Err(error);
                            }
                        }
                    },
                    None => break,
                }
            }
            let mut columns = batch.columns().to_vec();
            for &column in &self.indices {
                let input = batch.column(column);
                let values = (0..batch.num_rows()).map(|row| {
                    let failed = self.failure_policy == DownloadFailurePolicy::Null
                        && failed_cells.contains(&(column, row));
                    (input.is_valid(row) && !failed).then_some(b"".as_slice())
                });
                columns[column] = Arc::new(LargeBinaryArray::from_iter(values));
            }
            let mut output =
                RecordBatch::try_new(self.output_schema.clone(), columns).map_err(backend)?;
            if self.failure_policy == DownloadFailurePolicy::SkipRow && !failed_rows.is_empty() {
                let keep = BooleanArray::from_iter(
                    (0..batch.num_rows()).map(|row| Some(!failed_rows.contains(&row))),
                );
                output =
                    arrow_select::filter::filter_record_batch(&output, &keep).map_err(backend)?;
                let mut row_map = vec![None; batch.num_rows()];
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
