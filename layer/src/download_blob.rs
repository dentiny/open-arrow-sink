use std::{collections::HashSet, num::NonZeroUsize, sync::Arc};

use arrow_array::{
    Array, LargeStringArray, StringArray, StringViewArray, builder::LargeBinaryBuilder,
};
use arrow_schema::{DataType, Schema};
use futures::{StreamExt, TryStreamExt, stream};
use open_arrow_sink_core::{
    BLOB_METADATA_KEY, Layer, RecordBatch, Result, SchemaRef, Sink, SinkCapability, WriteOptions,
    WriteSummary, Writer, async_trait, validation,
};

use crate::{
    DownloadPool,
    error::{backend, failure},
    validation::validate_download_config,
};

/// Download explicit top-level URL columns over HTTP(S), replacing each with a
/// blob-marked LargeBinary column. Nulls and input row order are preserved.
/// Requires a Tokio runtime. Downloads share a global pool by default, without
/// retries or a request timeout. Use `with_download_pool` to supply another pool.
#[derive(Clone, Debug)]
pub struct DownloadBlobLayer {
    columns: HashSet<String>,
    max_blob_bytes: NonZeroUsize,
    rows_per_batch: NonZeroUsize,
    download_pool: Option<Arc<DownloadPool>>,
}

impl DownloadBlobLayer {
    /// Select URL columns by name; the set is resolved once when opening a writer.
    pub fn new(columns: HashSet<String>) -> Self {
        Self {
            columns,
            max_blob_bytes: NonZeroUsize::new(16 * 1024 * 1024).unwrap(),
            rows_per_batch: NonZeroUsize::new(16).unwrap(),
            download_pool: None,
        }
    }

    /// Share a caller-owned pool across any number of layers and writers.
    /// Otherwise, all layers share a process-wide pool of 16 active downloads.
    pub fn with_download_pool(mut self, pool: Arc<DownloadPool>) -> Self {
        self.download_pool = Some(pool);
        self
    }

    pub fn with_max_blob_bytes(mut self, bytes: NonZeroUsize) -> Self {
        self.max_blob_bytes = bytes;
        self
    }

    /// Bounds output buffering by rows, independently of the caller's batch size.
    pub fn with_rows_per_batch(mut self, rows: NonZeroUsize) -> Self {
        self.rows_per_batch = rows;
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
        self.inner.capability()
    }

    async fn open(
        &self,
        request: Self::Request,
        schema: SchemaRef,
        options: WriteOptions,
    ) -> Result<Box<dyn Writer>> {
        // Inner layers may override options (for example OrderingLayer), so leave
        // shared option validation to the inner sink, as RebatchLayer does.
        let indices = validate_download_config(&schema, &self.config.columns)?;
        let mut fields = schema.fields().to_vec();
        for &index in &indices {
            let field = schema.field(index);
            let mut metadata = field.metadata().clone();
            metadata.remove("ARROW:extension:name");
            metadata.remove("ARROW:extension:metadata");
            metadata.insert(BLOB_METADATA_KEY.into(), "true".into());
            fields[index] = Arc::new(
                field
                    .clone()
                    .with_data_type(DataType::LargeBinary)
                    .with_metadata(metadata),
            );
        }
        let output_schema = Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone()));
        let pool = match &self.config.download_pool {
            Some(pool) => pool.clone(),
            None => DownloadPool::global()?,
        };
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
            config: self.config.clone(),
            failed: false,
        }))
    }
}

struct DownloadBlobWriter {
    inner: Box<dyn Writer>,
    schema: SchemaRef,
    output_schema: SchemaRef,
    indices: Vec<usize>,
    pool: Arc<DownloadPool>,
    config: DownloadBlobLayer,
    failed: bool,
}

#[async_trait]
impl Writer for DownloadBlobWriter {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    async fn write(&mut self, batch: RecordBatch) -> Result<()> {
        if self.failed {
            return Err(failure("blob writer failed; abort or drop it"));
        }
        // Cancellation or any error poisons the session, including after earlier
        // output chunks have already been accepted by the inner writer.
        self.failed = true;
        validation::validate_batch(&self.schema, &batch)?;
        if self.indices.is_empty() {
            self.inner.write(batch).await?;
        } else {
            for start in (0..batch.num_rows()).step_by(self.config.rows_per_batch.get()) {
                let count = (batch.num_rows() - start).min(self.config.rows_per_batch.get());
                let slice = batch.slice(start, count);
                let mut columns = slice.columns().to_vec();
                let pool = &self.pool;
                let schema = &self.schema;
                let limit = self.config.max_blob_bytes.get();
                let jobs: Vec<_> = self
                    .indices
                    .iter()
                    .enumerate()
                    .flat_map(|(column, &index)| (0..count).map(move |row| (column, index, row)))
                    .collect();
                let jobs = jobs.into_iter().map(|(column, index, row)| {
                    let array = slice.column(index);
                    async move {
                        let bytes = if array.is_null(row) {
                            None
                        } else {
                            let url = match array.data_type() {
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
                            };
                            Some(pool.download(url, limit).await.map_err(|e| {
                                failure(format!(
                                    "download column {} row {}: {e}",
                                    schema.field(index).name(),
                                    start + row
                                ))
                            })?)
                        };
                        Ok::<_, open_arrow_sink_core::Error>((column, row, bytes))
                    }
                });
                let mut downloads = stream::iter(jobs).buffer_unordered(pool.concurrency());
                let mut values = vec![vec![None; count]; self.indices.len()];
                while let Some((column, row, bytes)) = downloads.try_next().await? {
                    values[column][row] = bytes;
                }
                for (&index, values) in self.indices.iter().zip(values) {
                    let mut builder = LargeBinaryBuilder::new();
                    for value in values {
                        builder.append_option(value);
                    }
                    columns[index] = Arc::new(builder.finish());
                }
                let output =
                    RecordBatch::try_new(self.output_schema.clone(), columns).map_err(backend)?;
                self.inner.write(output).await?;
            }
        }
        self.failed = false;
        Ok(())
    }

    async fn commit(self: Box<Self>) -> Result<WriteSummary> {
        if self.failed {
            let _ = self.inner.abort().await;
            return Err(failure("blob writer failed; session aborted"));
        }
        self.inner.commit().await
    }

    async fn abort(self: Box<Self>) -> Result<()> {
        self.inner.abort().await
    }
}
