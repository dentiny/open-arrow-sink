use std::{
    collections::HashSet,
    sync::{Arc, Mutex},
};

use arrow_array::{
    Array, LargeStringArray, StringArray, StringViewArray, builder::LargeBinaryBuilder,
};
use arrow_schema::{DataType, Schema};
use futures::{StreamExt, stream::FuturesUnordered};
use open_arrow_sink_core::{
    BLOB_METADATA_KEY, BLOB_METADATA_VALUE, BlobPlacement, BlobWriter, Error, GuardedWriter, Layer,
    RecordBatch, Result, SchemaRef, Sink, SinkCapability, WriteOptions, WriteSummary, Writer,
    async_trait, validation,
};

use crate::{DownloadPool, error, validation::validate_download_config};

/// Stream selected URL columns into native blobs and attach their descriptors.
/// Requires a sink supporting `Writer::create_blob` and `write_with_blobs`.
/// Input batch boundaries, row order, nulls, and unselected columns are preserved.
/// A default pool is initialized lazily and shared across opens and layer clones.
#[derive(Clone, Debug)]
pub struct DownloadBlobLayer {
    columns: HashSet<String>,
    download_pool: Arc<Mutex<Option<Arc<DownloadPool>>>>,
}
impl DownloadBlobLayer {
    pub fn new(columns: HashSet<String>) -> Self {
        Self {
            columns,
            download_pool: Arc::default(),
        }
    }
    /// Override the pool for this layer; existing clones retain their pool.
    pub fn with_download_pool(mut self, pool: Arc<DownloadPool>) -> Self {
        self.download_pool = Arc::new(Mutex::new(Some(pool)));
        self
    }

    fn pool(&self) -> Result<Arc<DownloadPool>> {
        let mut pool = self
            .download_pool
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(pool) = pool.as_ref() {
            return Ok(pool.clone());
        }
        let created = Arc::new(DownloadPool::with_defaults()?);
        *pool = Some(created.clone());
        Ok(created)
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
            metadata.insert(BLOB_METADATA_KEY.into(), BLOB_METADATA_VALUE.into());
            fields[column_index] = Arc::new(
                field
                    .clone()
                    .with_data_type(DataType::LargeBinary)
                    .with_metadata(metadata),
            );
        }
        let output_schema = Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone()));
        let pool = self.config.pool()?;
        let io_metrics = options.io_metrics.clone();
        let inner = self
            .inner
            .open(request, output_schema.clone(), options)
            .await?;
        Ok(Box::new(GuardedWriter::new(Box::new(DownloadBlobWriter {
            inner,
            schema,
            output_schema,
            indices,
            pool,
            io_metrics,
        }))))
    }
}

struct DownloadBlobWriter {
    inner: Box<dyn Writer>,
    schema: SchemaRef,
    output_schema: SchemaRef,
    indices: Vec<usize>,
    pool: Arc<DownloadPool>,
    io_metrics: Option<open_arrow_sink_core::IoMetrics>,
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
async fn download(
    pool: Arc<DownloadPool>,
    url: String,
    mut native: Box<dyn BlobWriter>,
    name: String,
    column: usize,
    row: usize,
    io_metrics: Option<open_arrow_sink_core::IoMetrics>,
) -> Result<BlobPlacement> {
    let result = async {
        let mut stream = pool.stream_with_metrics(&url, io_metrics).await?;
        while let Some(chunk) = stream.next().await {
            let chunk = chunk?;
            native.write(chunk).await?;
        }
        Ok(BlobPlacement {
            column,
            row,
            blob: native.finish().await?,
        })
    }
    .await
    .map_err(|error| error::download_context(&name, row, error))?;
    Ok(result)
}
#[async_trait]
impl Writer for DownloadBlobWriter {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }
    async fn write(&mut self, batch: RecordBatch) -> Result<()> {
        validation::validate_batch(&self.schema, &batch)?;
        if self.indices.is_empty() {
            self.inner.write(batch).await?;
        } else {
            let mut columns = batch.columns().to_vec();
            let mut jobs = Vec::new();
            for &column in &self.indices {
                let array = batch.column(column);
                let mut builder = LargeBinaryBuilder::new();
                for row in 0..batch.num_rows() {
                    if array.is_null(row) {
                        builder.append_null();
                    } else {
                        builder.append_value([]);
                        jobs.push((column, row));
                    }
                }
                columns[column] = Arc::new(builder.finish());
            }
            let mut jobs = jobs.into_iter();
            let mut active = FuturesUnordered::new();
            let mut blobs = Vec::new();
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
                    Some(result) => blobs.push(result?),
                    None => break,
                }
            }
            let output = RecordBatch::try_new(self.output_schema.clone(), columns)
                .map_err(Error::backend)?;
            self.inner.write_with_blobs(output, blobs).await?;
        }
        Ok(())
    }
    async fn commit(self: Box<Self>) -> Result<WriteSummary> {
        self.inner.commit().await
    }
    async fn abort(self: Box<Self>) -> Result<()> {
        self.inner.abort().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::TryStreamExt;

    #[tokio::test]
    async fn default_transfer_budget_is_shared_by_layer_clones() {
        let layer = DownloadBlobLayer::new(HashSet::new());
        // Clone before lazy initialization, as layer() does for each wrapped sink.
        let cloned = layer.clone();
        let file = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(file.path(), b"blob").unwrap();
        let url = open_arrow_sink_storage::Url::from_file_path(file.path()).unwrap();
        let first = layer.pool().unwrap();
        let second = cloned.pool().unwrap();
        let mut stream = first.stream(url.as_str()).await.unwrap();
        let chunk = stream.try_next().await.unwrap().unwrap();
        assert!(second.buffered_bytes() >= chunk.as_ref().len());
        drop(stream);
        // Consumer-held chunks retain their reservation after cancellation.
        assert!(second.buffered_bytes() > 0);
        assert_eq!(
            DownloadBlobLayer::new(HashSet::new())
                .pool()
                .unwrap()
                .buffered_bytes(),
            0
        );
        drop(chunk);
        assert_eq!(second.buffered_bytes(), 0);
    }
}
