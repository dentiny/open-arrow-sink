use std::{num::NonZeroUsize, sync::Arc};

use futures::{
    TryStreamExt,
    stream::{self, BoxStream},
};
use http::{Request, Response, Uri};
use once_cell::sync::OnceCell;
use open_arrow_sink_core::{BlobChunk, Error, Result};
use opendal::{
    Buffer, HttpBody, HttpTransport, HttpTransporter, Operator, raw::Layer, services::Http,
};
use tokio::sync::Semaphore;

use crate::{error::backend, validation};

// Default maximum number of active download streams across the process.
const DEFAULT_CONCURRENCY: NonZeroUsize = NonZeroUsize::new(64).unwrap();
// Default shared transfer-buffer budget: 8 GiB, including consumer-held chunks.
// Excludes OpenDAL/HTTP/TLS buffers and destination writer buffers.
const DEFAULT_BUFFERED_BYTES: NonZeroUsize = NonZeroUsize::new(8 * 1024 * 1024 * 1024).unwrap();
// Maximum buffer allocated for each read: 2 MiB, capped by the pool budget.
const CHUNK_BYTES: usize = 2 * 1024 * 1024;

/// Streaming downloads with a process-wide request limit and per-pool byte budget.
/// Yielded chunks hold their budget until dropped, even after stream cancellation.
/// OpenDAL/HTTP/TLS and destination buffers are outside this transfer-buffer budget.
#[derive(Debug)]
pub struct DownloadPool {
    operator: Operator,
    requests: Arc<Semaphore>,
    concurrency: NonZeroUsize,
    buffers: Arc<Semaphore>,
    max_buffered_bytes: NonZeroUsize,
}

impl DownloadPool {
    /// Create a pool with its own transfer-buffer budget. All pools share the
    /// process-wide request limiter. The first pool reads
    /// `OPEN_ARROW_SINK_DOWNLOAD_CONCURRENCY`, falling back to `concurrency` if
    /// unset or invalid; subsequent pools reuse that limit regardless of their
    /// `concurrency` argument.
    pub fn new(concurrency: NonZeroUsize, max_buffered_bytes: NonZeroUsize) -> Result<Self> {
        static REQUESTS: OnceCell<(NonZeroUsize, Arc<Semaphore>)> = OnceCell::new();
        validation::validate_download_limit(max_buffered_bytes)?;
        let (concurrency, requests) = REQUESTS.get_or_try_init(|| {
            let limit = env_or("OPEN_ARROW_SINK_DOWNLOAD_CONCURRENCY", concurrency);
            validation::validate_download_limit(limit)?;
            Ok::<_, Error>((limit, Arc::new(Semaphore::new(limit.get()))))
        })?;
        opendal::install_default();
        let operator =
            Operator::new(Http::default().endpoint("https://download.invalid")).map_err(backend)?;
        Ok(Self {
            operator,
            requests: requests.clone(),
            concurrency: *concurrency,
            buffers: Arc::new(Semaphore::new(max_buffered_bytes.get())),
            max_buffered_bytes,
        })
    }

    /// Configure OpenDAL policies. Add TimeoutLayer before RetryLayer so every
    /// retry attempt receives its own I/O deadline. Neither is added by default.
    pub fn with_layer(mut self, layer: impl Layer) -> Self {
        self.operator = self.operator.layer(layer);
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

    /// Return the process-wide default pool with an 8 GiB transfer-buffer budget.
    /// If no pool has initialized the request limiter, use the environment
    /// override or 64. Otherwise reuse the already initialized process limit.
    pub fn global() -> Result<Arc<Self>> {
        static POOL: OnceCell<Arc<DownloadPool>> = OnceCell::new();
        POOL.get_or_try_init(|| {
            Self::new(DEFAULT_CONCURRENCY, DEFAULT_BUFFERED_BYTES).map(Arc::new)
        })
        .cloned()
    }

    /// Open a pull-based download. Dropping the stream cancels pending I/O and
    /// releases its request slot; already yielded chunks retain their reservations.
    pub async fn stream(&self, source: &str) -> Result<BoxStream<'static, Result<BlobChunk>>> {
        let uri = validation::validate_download_url(source)?;
        let request = self
            .requests
            .clone()
            .acquire_owned()
            .await
            .expect("pool is never closed");
        let context = self.operator.base_context();
        let transport = HttpTransporter::new(UrlTransport {
            uri,
            inner: context.http_transport().clone(),
        });
        // Replays timeout/retry layers outside the complete-URL adapter.
        let operator = self
            .operator
            .clone()
            .with_context(context.with_http_transport(transport));
        let reader = operator
            .reader("blob")
            .await
            .map_err(backend)?
            .into_stream(..)
            .await
            .map_err(backend)?;
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
                    let Some(buffer) = reader.try_next().await.map_err(backend)? else { break };
                    pending = buffer;
                }
                // A bounded copy avoids retaining a larger OpenDAL backing allocation.
                let size = capacity.min(pending.len());
                yield BlobChunk::with_guard(pending.split_to(size).to_vec(), reservation);
            }
        }))
    }
}

fn env_or(name: &str, default: NonZeroUsize) -> NonZeroUsize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
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
        if response.status().is_success() {
            return Ok(response);
        }
        // OpenDAL classifies errors after collecting their bodies. Discard that
        // diagnostic body so an arbitrarily large error cannot be collected.
        let (parts, _) = response.into_parts();
        Ok(Response::from_parts(
            parts,
            HttpBody::new(stream::empty(), Some(0)),
        ))
    }
}
