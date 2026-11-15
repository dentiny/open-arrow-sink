use std::{
    num::NonZeroUsize,
    sync::{Arc, OnceLock},
};

use futures::{
    AsyncReadExt,
    stream::{self, BoxStream},
};
use http::{Request, Response, Uri};
use open_arrow_sink_core::{BlobChunk, Error, Result};
use opendal::{
    Buffer, HttpBody, HttpTransport, HttpTransporter, Operator, raw::Layer, services::Http,
};
use tokio::sync::Semaphore;

use crate::error::{backend, failure};

const DEFAULT_CONCURRENCY: NonZeroUsize = NonZeroUsize::new(16).unwrap();
const DEFAULT_BUFFERED_BYTES: NonZeroUsize = NonZeroUsize::new(64 * 1024 * 1024).unwrap();
const CHUNK_BYTES: usize = 64 * 1024;

/// Shared streaming downloads with concurrency and payload-buffer budgets.
///
/// All streams from an `Arc<DownloadPool>` share both limits. Reservations are
/// acquired before allocating/reading a transfer buffer, travel with `BlobChunk`,
/// and are released only when the consumer drops that chunk. The budget covers
/// application-owned transfer buffers, including chunks retained by consumers;
/// HTTP/TLS transport buffers and the destination's internal buffers are separate.
/// A blob may exceed the budget because no complete payload is collected here.
#[derive(Debug)]
pub struct DownloadPool {
    operator: Operator,
    requests: Arc<Semaphore>,
    buffers: Arc<Semaphore>,
    concurrency: NonZeroUsize,
    max_buffered_bytes: NonZeroUsize,
}

impl DownloadPool {
    pub fn new(concurrency: NonZeroUsize, max_buffered_bytes: NonZeroUsize) -> Result<Self> {
        if concurrency.get() > Semaphore::MAX_PERMITS
            || max_buffered_bytes.get() > Semaphore::MAX_PERMITS
        {
            return Err(Error::Unsupported(
                "download pool limits exceed semaphore capacity".into(),
            ));
        }
        opendal::install_default();
        let operator =
            Operator::new(Http::default().endpoint("https://download.invalid")).map_err(backend)?;
        Ok(Self {
            operator,
            requests: Arc::new(Semaphore::new(concurrency.get())),
            buffers: Arc::new(Semaphore::new(max_buffered_bytes.get())),
            concurrency,
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

    pub fn global() -> Result<Arc<Self>> {
        static POOL: OnceLock<Arc<DownloadPool>> = OnceLock::new();
        if let Some(pool) = POOL.get() {
            return Ok(pool.clone());
        }
        let pool = Arc::new(Self::new(DEFAULT_CONCURRENCY, DEFAULT_BUFFERED_BYTES)?);
        Ok(POOL.get_or_init(|| pool).clone())
    }

    /// Open a pull-based download. Dropping the stream cancels pending I/O and
    /// releases its request slot; already yielded chunks retain their reservations.
    pub async fn stream(
        self: &Arc<Self>,
        source: &str,
    ) -> Result<BoxStream<'static, Result<BlobChunk>>> {
        let mut url = url::Url::parse(source).map_err(|_| failure("invalid blob URL"))?;
        if !matches!(url.scheme(), "http" | "https") {
            return Err(failure("blob URL must use HTTP or HTTPS"));
        }
        url.set_fragment(None);
        let uri: Uri = url
            .as_str()
            .parse()
            .map_err(|_| failure("invalid blob URL"))?;
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
            .into_futures_async_read(..)
            .await
            .map_err(backend)?;
        let pool = self.clone();
        Ok(Box::pin(stream::try_unfold(
            (reader, pool, request),
            |(mut reader, pool, request)| async move {
                let capacity = CHUNK_BYTES.min(pool.max_buffered_bytes.get());
                let reservation = pool
                    .buffers
                    .clone()
                    .acquire_many_owned(capacity as u32)
                    .await
                    .expect("pool is never closed");
                let mut buffer = vec![0; capacity];
                let read = reader.read(&mut buffer).await.map_err(backend)?;
                if read == 0 {
                    return Ok(None);
                }
                buffer.truncate(read);
                let chunk = BlobChunk::with_guard(buffer, reservation);
                Ok(Some((chunk, (reader, pool, request))))
            },
        )))
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
