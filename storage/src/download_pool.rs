use std::{num::NonZeroUsize, sync::Arc};

use futures::{TryStreamExt, stream::BoxStream};
use http::{Request, Response, Uri};
use open_arrow_sink_core::{BlobChunk, Result};
use opendal::{
    Buffer, HttpBody, HttpTransport, HttpTransporter, Operator, raw::Layer, services::Http,
};
use tokio::sync::Semaphore;

use crate::{error, validation};

// Default maximum number of active download streams in one pool.
const DEFAULT_CONCURRENCY: NonZeroUsize = NonZeroUsize::new(64).unwrap();
// Default shared transfer-buffer budget: 8 GiB, including consumer-held chunks.
// Excludes OpenDAL/HTTP/TLS buffers and destination writer buffers.
const DEFAULT_BUFFERED_BYTES: NonZeroUsize = NonZeroUsize::new(8 * 1024 * 1024 * 1024).unwrap();
// Maximum buffer allocated for each read: 2 MiB, capped by the pool budget.
const CHUNK_BYTES: usize = 2 * 1024 * 1024;

/// Streaming downloads with explicit per-pool stream and transfer-buffer budgets.
/// Share an `Arc<DownloadPool>` to share both budgets across writers.
/// Yielded chunks hold their budget until dropped, even after stream cancellation.
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
    /// Create independent stream and transfer-buffer budgets. Every call honors
    /// `concurrency`; no process-wide state or environment configuration is used.
    /// To share only an HTTP request budget across pools (or with `Storage`),
    /// install clones of one `HttpConcurrencyLimitLayer` with `with_layer`.
    pub fn new(concurrency: NonZeroUsize, max_buffered_bytes: NonZeroUsize) -> Result<Self> {
        validation::validate_http_concurrency(concurrency.get())?;
        validation::validate_download_limit(max_buffered_bytes)?;
        opendal::install_default();
        let operator =
            Operator::new(Http::default().endpoint("https://download.invalid")).map_err(Error::backend)?;
        Ok(Self {
            operator,
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
    /// retry attempt receives its own I/O deadline. Neither is added by default.
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
        let url = crate::Url::parse(source).map_err(Error::backend)?;
        let request = self
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
            .map_err(Error::backend)?
            .into_stream(..)
            .await
            .map_err(Error::backend)?;
        let buffers = self.buffers.clone();
        let capacity = CHUNK_BYTES.min(self.max_buffered_bytes.get());
        Ok(Box::pin(async_stream::try_stream! {
            let _request = request;
            let mut reader = reader;
            let mut pending = Buffer::new();
            loop {
                let reservation = buffers.clone().acquire_many_owned(capacity as u32)
                    .await.expect("pool is never closed");
                if pending.is_empty() {
                    let Some(buffer) = reader.try_next().await.map_err(Error::backend)? else { break };
                    pending = buffer;
                }
                // A bounded copy avoids retaining a larger OpenDAL backing allocation.
                let size = capacity.min(pending.len());
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
}

impl HttpTransport for UrlTransport {
    async fn fetch(&self, mut request: Request<Buffer>) -> opendal::Result<Response<HttpBody>> {
        *request.uri_mut() = self.uri.clone();
        let response = self.inner.fetch(request).await?;
        Ok(error::download_response(response))
    }
}
