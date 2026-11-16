use std::{
    num::NonZeroUsize,
    sync::{Arc, Mutex},
    time::Duration,
};

use futures::{TryStreamExt, stream::BoxStream};
use http::{Request, Response, Uri};
use open_arrow_sink_core::{BlobChunk, Result};
use opendal::{
    Buffer, HttpBody, HttpTransport, HttpTransporter, Operator, raw::Layer, services::Http,
};
use tokio::sync::Semaphore;

use crate::{
    error::{self, backend},
    validation,
};

// Default maximum number of active download streams in one pool.
const DEFAULT_CONCURRENCY: NonZeroUsize = NonZeroUsize::new(64).unwrap();
// Default shared transfer-buffer budget: 8 GiB, including consumer-held chunks.
// Excludes OpenDAL/HTTP/TLS buffers and destination writer buffers.
const DEFAULT_BUFFERED_BYTES: NonZeroUsize = NonZeroUsize::new(8 * 1024 * 1024 * 1024).unwrap();
// Maximum buffer allocated for each read: 2 MiB, capped by the pool budget.
const CHUNK_BYTES: usize = 2 * 1024 * 1024;

/// Streaming downloads with explicit per-pool stream and transfer-buffer budgets.
/// Share an `Arc<DownloadPool>` to share both budgets across writers.
/// Yielded chunks reserve their actual byte length until dropped, even after stream cancellation.
/// OpenDAL/HTTP/TLS and destination buffers are outside this transfer-buffer budget.
#[derive(Debug)]
pub struct DownloadPool {
    operator: Operator,
    sources: crate::StorageResolver,
    // Bound lazy streams, including non-HTTP sources, until cancellation or EOF.
    stream_slots: Arc<Semaphore>,
    concurrency: NonZeroUsize,
    buffers: Arc<Semaphore>,
    max_buffered_bytes: NonZeroUsize,
}

impl DownloadPool {
    /// Create independent per-pool stream and transfer-buffer budgets.
    /// HTTP control and I/O attempts time out after eight seconds, with up to
    /// three retries for temporary errors. This is not a whole-object deadline;
    /// use `new_with_config` to override these policies.
    pub fn new(concurrency: NonZeroUsize, max_buffered_bytes: NonZeroUsize) -> Result<Self> {
        Self::new_with_config(
            concurrency,
            max_buffered_bytes,
            crate::StorageConfig {
                timeout: Some(crate::TimeoutConfig {
                    operation_timeout: Duration::from_secs(8),
                    io_timeout: Duration::from_secs(8),
                }),
                retry: Some(crate::RetryConfig::default()),
            },
        )
    }

    /// Override HTTP download policies. `StorageConfig::default()` explicitly
    /// disables timeouts and retries. Non-HTTP source stores retain their own policies.
    pub fn new_with_config(
        concurrency: NonZeroUsize,
        max_buffered_bytes: NonZeroUsize,
        config: crate::StorageConfig,
    ) -> Result<Self> {
        config.validate()?;
        validation::validate_http_concurrency(concurrency.get())?;
        validation::validate_download_limit(max_buffered_bytes)?;
        opendal::install_default();
        let operator =
            Operator::new(Http::default().endpoint("https://download.invalid")).map_err(backend)?;
        Ok(Self {
            operator: config.apply(operator)?,
            sources: crate::StorageResolver::default(),
            stream_slots: Arc::new(Semaphore::new(concurrency.get())),
            concurrency,
            buffers: Arc::new(Semaphore::new(max_buffered_bytes.get())),
            max_buffered_bytes,
        })
    }

    /// Use a caller-provided HTTP transport for authentication, instrumentation,
    /// or deterministic tests. Existing concurrency and policy layers are replayed
    /// around this transport, including any explicitly shared HTTP request budget.
    pub fn with_http_transport(mut self, transport: HttpTransporter) -> Self {
        let context = self.operator.base_context().with_http_transport(transport);
        self.operator = self.operator.with_context(context);
        self
    }

    /// Configure OpenDAL policies. Add TimeoutLayer before RetryLayer so every
    /// retry attempt receives its own I/O deadline. These stack on the constructor
    /// policies; use `new_with_config` to replace or disable those policies.
    pub fn with_layer(mut self, layer: impl Layer) -> Self {
        self.operator = self.operator.layer(layer);
        self
    }

    /// Register a source store for non-HTTP downloads, including custom S3 credentials.
    pub fn with_source_storage(mut self, storage: crate::Storage) -> Self {
        self.sources = self.sources.with_storage(storage);
        self
    }

    pub fn concurrency(&self) -> usize {
        self.concurrency.get()
    }
    pub fn max_buffered_bytes(&self) -> usize {
        self.max_buffered_bytes.get()
    }
    pub fn buffered_bytes(&self) -> usize {
        self.max_buffered_bytes.get() - self.buffers.available_permits()
    }

    /// Create an independent pool with 64 stream slots and an 8 GiB
    /// transfer-buffer budget. Share it explicitly with `Arc` when needed.
    pub fn with_defaults() -> Result<Self> {
        Self::new(DEFAULT_CONCURRENCY, DEFAULT_BUFFERED_BYTES)
    }

    /// Open a pull-based download. Dropping the stream cancels pending I/O and
    /// releases its stream slot; already yielded chunks retain their reservations.
    pub async fn stream(&self, source: &str) -> Result<BoxStream<'static, Result<BlobChunk>>> {
        let url = crate::Url::parse(source).map_err(backend)?;
        let stream_slot = self
            .stream_slots
            .clone()
            .acquire_owned()
            .await
            .expect("pool is never closed");
        let (operator, path) = if matches!(url.scheme(), "http" | "https") {
            let uri = validation::validate_download_url(source)?;
            let context = self.operator.base_context();
            let transport = HttpTransporter::new(UrlTransport {
                uri,
                inner: context.http_transport().clone(),
                validator: Arc::new(Mutex::new(None)),
            });
            (
                self.operator
                    .clone()
                    .with_context(context.with_http_transport(transport)),
                "blob".to_owned(),
            )
        } else {
            let storage = self.sources.resolve(&url)?;
            (storage.operator(), storage.path(&url)?)
        };
        let reader = operator
            .reader(&path)
            .await
            .map_err(backend)?
            .into_stream(..)
            .await
            .map_err(backend)?;
        let buffers = self.buffers.clone();
        let capacity = CHUNK_BYTES.min(self.max_buffered_bytes.get());
        Ok(Box::pin(async_stream::try_stream! {
            let _stream_slot = stream_slot;
            let mut reader = reader;
            let mut pending = Buffer::new();
            loop {
                if pending.is_empty() {
                    let Some(buffer) = reader.try_next().await.map_err(backend)? else { break };
                    pending = buffer;
                }
                let size = capacity.min(pending.len());
                // Native read buffers are outside the transfer budget. Once their
                // size is known, reserve only the bytes exposed to the consumer.
                let reservation = buffers.clone().acquire_many_owned(size as u32)
                    .await.expect("pool is never closed");
                // A bounded copy avoids retaining a larger OpenDAL backing allocation.
                yield BlobChunk::with_guard(pending.split_to(size).to_vec(), reservation);
            }
        }))
    }
}

// The HTTP service encodes object paths. Replace its synthetic object URI with
// the complete source URL so signed queries, escaped slashes, and redirects work.
struct UrlTransport {
    uri: Uri,
    inner: HttpTransporter,
    validator: Arc<Mutex<Option<String>>>,
}

impl HttpTransport for UrlTransport {
    async fn fetch(&self, mut request: Request<Buffer>) -> opendal::Result<Response<HttpBody>> {
        *request.uri_mut() = self.uri.clone();
        let offset = request
            .headers()
            .get(http::header::RANGE)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.strip_prefix("bytes="))
            .and_then(|value| value.split_once('-'))
            .and_then(|(start, _)| start.parse::<u64>().ok())
            .unwrap_or(0);
        let validator = self.validator.lock().unwrap().clone();
        if offset > 0
            && let Some(validator) = &validator
        {
            request.headers_mut().insert(
                http::header::IF_RANGE,
                validator.parse().expect("previously validated HTTP header"),
            );
        }
        let response = self.inner.fetch(request).await?;
        if response.status().is_success() {
            if offset > 0 {
                let actual_start = response
                    .headers()
                    .get(http::header::CONTENT_RANGE)
                    .and_then(|value| value.to_str().ok())
                    .and_then(|value| value.strip_prefix("bytes "))
                    .and_then(|value| value.split_once('-'))
                    .and_then(|(start, _)| start.parse::<u64>().ok());
                let changed = validator.as_ref().is_some_and(|expected| {
                    response
                        .headers()
                        .get(http::header::ETAG)
                        .is_some_and(|actual| actual != expected.as_str())
                });
                if response.status() != http::StatusCode::PARTIAL_CONTENT
                    || actual_start != Some(offset)
                    || changed
                {
                    return Err(opendal::Error::new(
                        opendal::ErrorKind::Unexpected,
                        "HTTP download cannot safely resume: invalid range response or changed ETag",
                    ));
                }
            } else if let Some(etag) = response
                .headers()
                .get(http::header::ETAG)
                .and_then(|value| value.to_str().ok())
                .filter(|value| !value.starts_with("W/"))
            {
                *self.validator.lock().unwrap() = Some(etag.to_owned());
            }
        }
        Ok(error::download_response(response))
    }
}
