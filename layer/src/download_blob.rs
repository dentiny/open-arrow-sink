use std::{collections::HashSet, io, num::NonZeroUsize, sync::Arc, time::Duration};

use arrow_array::{
    Array, LargeStringArray, StringArray, StringViewArray, builder::LargeBinaryBuilder,
};
use arrow_schema::{DataType, Schema};
use open_arrow_sink_core::{
    BLOB_METADATA_KEY, Error, Layer, RecordBatch, Result, SchemaRef, Sink, SinkCapability,
    WriteOptions, WriteSummary, Writer, async_trait, validation,
};

fn backend(error: impl std::error::Error + Send + Sync + 'static) -> Error {
    Error::Backend(Box::new(error))
}

fn failure(message: impl Into<String>) -> Error {
    backend(io::Error::other(message.into()))
}

/// Download explicit top-level URL columns over HTTP(S), replacing each with a
/// blob-marked LargeBinary column. Nulls and input row order are preserved.
/// Requires a Tokio runtime. Downloads run sequentially without retries.
#[derive(Clone, Debug)]
pub struct DownloadBlobLayer {
    columns: Vec<String>,
    max_blob_bytes: NonZeroUsize,
    rows_per_batch: NonZeroUsize,
    timeout: Duration,
}

impl DownloadBlobLayer {
    pub fn new(columns: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self {
            columns: columns.into_iter().map(Into::into).collect(),
            max_blob_bytes: NonZeroUsize::new(16 * 1024 * 1024).unwrap(),
            rows_per_batch: NonZeroUsize::new(16).unwrap(),
            timeout: Duration::from_secs(30),
        }
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

    /// Total timeout for each request, including the response body. Must be positive.
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
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
        if self.config.timeout.is_zero() {
            return Err(Error::Unsupported(
                "download timeout must be positive".into(),
            ));
        }
        let mut seen = HashSet::new();
        let mut indices = Vec::new();
        let mut fields = schema.fields().to_vec();
        for name in &self.config.columns {
            let matches: Vec<_> = schema
                .fields()
                .iter()
                .enumerate()
                .filter(|(_, f)| f.name() == name)
                .collect();
            if !seen.insert(name) || matches.len() != 1 {
                return Err(Error::Unsupported(format!(
                    "download column must exist and be unique: {name}"
                )));
            }
            let (index, field) = matches[0];
            if !matches!(
                field.data_type(),
                DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
            ) {
                return Err(Error::Unsupported(format!(
                    "download column must be a string: {name}"
                )));
            }
            let mut metadata = field.metadata().clone();
            metadata.remove("ARROW:extension:name");
            metadata.remove("ARROW:extension:metadata");
            metadata.insert(BLOB_METADATA_KEY.into(), "true".into());
            fields[index] = Arc::new(
                field
                    .as_ref()
                    .clone()
                    .with_data_type(DataType::LargeBinary)
                    .with_metadata(metadata),
            );
            indices.push(index);
        }
        let output_schema = Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone()));
        let client = reqwest::Client::builder()
            .timeout(self.config.timeout)
            .build()
            .map_err(backend)?;
        let inner = self
            .inner
            .open(request, output_schema.clone(), options)
            .await?;
        Ok(Box::new(DownloadBlobWriter {
            inner,
            schema,
            output_schema,
            indices,
            client,
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
    client: reqwest::Client,
    config: DownloadBlobLayer,
    failed: bool,
}

impl DownloadBlobWriter {
    async fn download(client: &reqwest::Client, limit: usize, url: &str) -> Result<Vec<u8>> {
        let url = reqwest::Url::parse(url).map_err(|_| failure("invalid blob URL"))?;
        if !matches!(url.scheme(), "http" | "https") {
            return Err(failure("blob URL must use HTTP or HTTPS"));
        }
        let mut response = client
            .get(url)
            .send()
            .await
            .map_err(|e| backend(e.without_url()))?
            .error_for_status()
            .map_err(|e| backend(e.without_url()))?;
        if !response.status().is_success() {
            return Err(failure(format!(
                "unexpected HTTP status: {}",
                response.status()
            )));
        }
        if response.content_length().is_some_and(|n| n > limit as u64) {
            return Err(failure("blob exceeds max_blob_bytes"));
        }
        let mut bytes = Vec::new();
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|e| backend(e.without_url()))?
        {
            if chunk.len() > limit - bytes.len() {
                return Err(failure("blob exceeds max_blob_bytes"));
            }
            bytes.extend_from_slice(&chunk);
        }
        Ok(bytes)
    }
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
                for &index in &self.indices {
                    let array = slice.column(index);
                    let mut builder = LargeBinaryBuilder::new();
                    for row in 0..count {
                        if array.is_null(row) {
                            builder.append_null();
                            continue;
                        }
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
                        let bytes =
                            Self::download(&self.client, self.config.max_blob_bytes.get(), url)
                                .await
                                .map_err(|e| {
                                    failure(format!(
                                        "download column {} row {}: {e}",
                                        self.schema.field(index).name(),
                                        start + row
                                    ))
                                })?;
                        builder.append_value(bytes);
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
