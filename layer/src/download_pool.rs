use std::{num::NonZeroUsize, sync::Arc};

use futures::{TryStreamExt, stream::BoxStream};
use http::{Request, Response, Uri};
use open_arrow_sink_core::{BlobChunk, Error, Result};
use open_arrow_sink_storage::{Storage, StorageConfig, StorageResolver, Url, opendal};
use opendal::{Buffer, HttpBody, HttpTransport, HttpTransporter, Operator, services::Http};
use tokio::sync::Semaphore;

// OpenDAL's HTTP service needs an endpoint and encodes object paths. This is
// only an internal reader template; UrlTransport sends the complete source URL.
const HTTP_PLACEHOLDER: &str = "https://download.invalid/";

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
    http: Operator,
    transport: HttpTransporter,
    sources: Arc<StorageResolver>,
    // Bound lazy streams, including non-HTTP sources, until cancellation or EOF.
    stream_slots: Arc<Semaphore>,
    concurrency: NonZeroUsize,
    buffers: Arc<Semaphore>,
    max_buffered_bytes: NonZeroUsize,
}

impl DownloadPool {
    /// Create independent stream and transfer-buffer budgets. Every call honors
    /// `concurrency`; no process-wide state or environment configuration is used.
    /// HTTP(S) downloads share the stream budget without a second request limiter.
    pub fn new(concurrency: NonZeroUsize, max_buffered_bytes: NonZeroUsize) -> Result<Self> {
        validate_limit(concurrency)?;
        validate_limit(max_buffered_bytes)?;
        let http = http_operator(StorageConfig::default())?;
        let transport = http.base_context().http_transport().clone();
        Ok(Self {
            http,
            transport,
            sources: Arc::default(),
            stream_slots: Arc::new(Semaphore::new(concurrency.get())),
            concurrency,
            buffers: Arc::new(Semaphore::new(max_buffered_bytes.get())),
            max_buffered_bytes,
        })
    }

    /// Apply policies to HTTP downloads and URL-resolved non-HTTP sources.
    /// Explicit source stores retain their own configuration. Detach resolver
    /// configuration so other consumers of a shared resolver keep their policies.
    pub fn with_config(mut self, config: StorageConfig) -> Result<Self> {
        self.sources = Arc::new(self.sources.as_ref().clone().with_config(config.clone())?);
        self.http = config.apply(self.http)?;
        Ok(self)
    }

    /// Replace HTTP retry/timeout policies. Each retry attempt gets its own timeout.
    /// The pool's stream budget and provider transport are retained.
    pub fn with_http_config(mut self, config: StorageConfig) -> Result<Self> {
        self.http = http_operator(config)?;
        Ok(self)
    }

    /// Replace the HTTP provider transport for authentication, instrumentation,
    /// or deterministic tests. Stream and byte budgets and policies still apply.
    pub fn with_http_transport(mut self, transport: HttpTransporter) -> Self {
        self.transport = transport;
        self
    }

    /// Share source namespaces, credentials, and the URL-derived cache with other consumers.
    /// This replaces any previously configured non-HTTP source stores.
    pub fn with_source_resolver(mut self, sources: Arc<StorageResolver>) -> Self {
        self.sources = sources;
        self
    }

    /// Register a source store for non-HTTP downloads, including custom S3 credentials.
    /// If the resolver is shared, detach its registrations before adding this store.
    pub fn with_source_storage(mut self, storage: Storage) -> Self {
        self.sources = Arc::new(self.sources.as_ref().clone().with_storage(storage));
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
        let url = Url::parse(source).map_err(Error::backend)?;
        let stream_slot = self
            .stream_slots
            .clone()
            .acquire_owned()
            .await
            .expect("pool is never closed");
        let (operator, path) = if matches!(url.scheme(), "http" | "https") {
            let uri = validate_download_url(source)?;
            let operator = self.http.clone();
            let context = operator.base_context();
            let transport = HttpTransporter::new(UrlTransport {
                uri,
                inner: self.transport.clone(),
            });
            let context = context.with_http_transport(transport);
            (operator.with_context(context), "blob".to_owned())
        } else {
            let storage = self.sources.resolve(&url)?;
            (
                storage.operator(),
                storage.resolve(&url)?.as_str().to_owned(),
            )
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
            let _stream_slot = stream_slot;
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
        Ok(download_response(response))
    }
}

fn http_operator(config: StorageConfig) -> Result<Operator> {
    opendal::install_default();
    let operator = Operator::new(Http::default().endpoint(HTTP_PLACEHOLDER.trim_end_matches('/')))
        .map_err(Error::backend)?;
    config.apply(operator)
}

fn invalid(message: &str) -> Error {
    Error::backend(std::io::Error::new(
        std::io::ErrorKind::InvalidInput,
        message,
    ))
}

fn validate_limit(limit: NonZeroUsize) -> Result<()> {
    if limit.get() > Semaphore::MAX_PERMITS {
        return Err(invalid("download pool limits exceed semaphore capacity"));
    }
    Ok(())
}

fn validate_download_url(source: &str) -> Result<Uri> {
    let mut url = Url::parse(source).map_err(|_| invalid("invalid blob URL"))?;
    if !matches!(url.scheme(), "http" | "https") {
        return Err(invalid("blob URL must use HTTP or HTTPS"));
    }
    url.set_fragment(None);
    url.as_str()
        .parse()
        .map_err(|_| invalid("invalid blob URL"))
}

fn download_response(response: Response<HttpBody>) -> Response<HttpBody> {
    if response.status().is_success() {
        return response;
    }
    // OpenDAL collects error bodies for diagnostics. Discard an unbounded body.
    let (parts, _) = response.into_parts();
    Response::from_parts(parts, HttpBody::new(futures::stream::empty(), Some(0)))
}
