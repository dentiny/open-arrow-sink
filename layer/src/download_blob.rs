use std::{collections::HashSet, sync::Arc};

use arrow_array::{
    Array, LargeStringArray, StringArray, StringViewArray, builder::LargeBinaryBuilder,
};
use arrow_schema::{DataType, Schema};
use futures::{StreamExt, stream::FuturesUnordered};
use open_arrow_sink_core::{
    BLOB_METADATA_KEY, BlobPlacement, BlobWriter, Layer, RecordBatch, Result, SchemaRef, Sink,
    SinkCapability, WriteOptions, WriteSummary, Writer, async_trait, validation,
};

use crate::{
    DownloadPool,
    error::{self, backend},
    validation::validate_download_config,
};

/// Stream selected URL columns into native blobs and attach their descriptors.
/// Requires a sink supporting `Writer::create_blob` and `write_with_blobs`.
/// Input batch boundaries, row order, nulls, and unselected columns are preserved.
#[derive(Clone, Debug)]
pub struct DownloadBlobLayer {
    columns: HashSet<String>,
    download_pool: Option<Arc<DownloadPool>>,
}
impl DownloadBlobLayer {
    pub fn new(columns: HashSet<String>) -> Self {
        Self {
            columns,
            download_pool: None,
        }
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
    failed: bool,
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
) -> Result<BlobPlacement> {
    let result = async {
        let mut stream = pool.stream(&url).await?;
        while let Some(chunk) = stream.next().await {
            native.write(chunk?).await?;
        }
        native.finish().await
    }
    .await
    .map_err(|error| error::download_context(&name, row, error))?;
    Ok(BlobPlacement {
        column,
        row,
        blob: result,
    })
}
#[async_trait]
impl Writer for DownloadBlobWriter {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }
    async fn write(&mut self, batch: RecordBatch) -> Result<()> {
        crate::validation::validate_download_writer(self.failed)?;
        self.failed = true;
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
                    ));
                }
                match active.next().await {
                    Some(result) => blobs.push(result?),
                    None => break,
                }
            }
            let output =
                RecordBatch::try_new(self.output_schema.clone(), columns).map_err(backend)?;
            self.inner.write_with_blobs(output, blobs).await?;
        }
        self.failed = false;
        Ok(())
    }
    async fn commit(self: Box<Self>) -> Result<WriteSummary> {
        if crate::validation::validate_download_writer(self.failed).is_err() {
            let _ = self.inner.abort().await;
            return Err(error::download_aborted());
        }
        self.inner.commit().await
    }
    async fn abort(self: Box<Self>) -> Result<()> {
        self.inner.abort().await
    }
}
