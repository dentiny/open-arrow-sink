use std::{
    num::NonZeroUsize,
    sync::{Arc, OnceLock},
};

use futures::{StreamExt, TryStreamExt};
use http::{Request, Response, Uri};
use open_arrow_sink_core::{Error, Result};
use opendal::{
    Buffer, HttpBody, HttpTransport, HttpTransporter, Operator, raw::Layer, services::Http,
};
use tokio::sync::Semaphore;

use crate::{
    error::{backend, failure},
    validation::{validate_blob_chunk, validate_blob_url},
};

const DEFAULT_DOWNLOAD_CONCURRENCY: NonZeroUsize = NonZeroUsize::new(16).unwrap();

/// Shared OpenDAL operator and concurrency budget for blob downloads.
///
/// Cloning an `Arc<DownloadPool>` shares connections, OpenDAL layers, and the
/// download limit across layers and writers. Downloads run on the caller's Tokio
/// runtime and are cancelled when the write future is dropped. No retries or
/// timeout are configured by default; use `with_layer` to configure OpenDAL.
#[derive(Debug)]
pub struct DownloadPool {
    operator: Operator,
    permits: Semaphore,
    concurrency: NonZeroUsize,
}

impl DownloadPool {
    /// Create an independent pool with at most `concurrency` active downloads.
    /// A permit covers the entire response body, including retries.
    pub fn new(concurrency: NonZeroUsize) -> Result<Self> {
        if concurrency.get() > Semaphore::MAX_PERMITS {
            return Err(Error::Unsupported(
                "download concurrency exceeds semaphore capacity".into(),
            ));
        }
        opendal::install_default();
        // Each read replaces this endpoint with its complete source URL through
        // UrlTransport, preserving escaped paths, trailing slashes, and queries.
        let operator =
            Operator::new(Http::default().endpoint("https://download.invalid")).map_err(backend)?;
        Ok(Self {
            operator,
            permits: Semaphore::new(concurrency.get()),
            concurrency,
        })
    }

    /// Apply an OpenDAL layer to every download in this pool.
    ///
    /// For timeouts and retries, add `TimeoutLayer` before `RetryLayer`, so each
    /// attempt has a timeout and the retry layer can restore its reader state.
    pub fn with_layer(mut self, layer: impl Layer) -> Self {
        self.operator = self.operator.layer(layer);
        self
    }

    pub(crate) fn concurrency(&self) -> usize {
        self.concurrency.get()
    }

    pub(crate) fn global() -> Result<Arc<Self>> {
        static POOL: OnceLock<Arc<DownloadPool>> = OnceLock::new();
        if let Some(pool) = POOL.get() {
            return Ok(pool.clone());
        }
        let pool = Arc::new(Self::new(DEFAULT_DOWNLOAD_CONCURRENCY)?);
        Ok(POOL.get_or_init(|| pool).clone())
    }

    pub(crate) async fn download(&self, url: &str, limit: usize) -> Result<Vec<u8>> {
        let mut url = validate_blob_url(url)?;
        url.set_fragment(None);
        let uri: Uri = url
            .as_str()
            .parse()
            .map_err(|_| failure("invalid blob URL"))?;
        // The semaphore is private and never closed. Cancellation drops both the
        // OpenDAL stream (including pending retries) and this permit.
        let _permit = self.permits.acquire().await.expect("pool is never closed");
        let context = self.operator.base_context();
        let transport = HttpTransporter::new(UrlTransport {
            uri,
            limit,
            inner: context.http_transport().clone(),
        });
        // with_context replays the configured layers over the new base context,
        // keeping OpenDAL timeout/retry wrappers outside the URL adapter.
        let operator = self
            .operator
            .clone()
            .with_context(context.with_http_transport(transport));
        let reader = operator.reader("blob").await.map_err(backend)?;
        let mut stream = reader.into_stream(..).await.map_err(backend)?;
        let metadata = stream.metadata().await.map_err(backend)?;
        if metadata.content_length() > limit as u64 {
            return Err(failure("blob exceeds max_blob_bytes"));
        }
        let mut bytes = Vec::new();
        while let Some(chunk) = stream.try_next().await.map_err(backend)? {
            validate_blob_chunk(bytes.len(), chunk.len(), limit)?;
            for segment in chunk {
                bytes.extend_from_slice(&segment);
            }
        }
        Ok(bytes)
    }
}

// OpenDAL's HTTP service treats object paths as filesystem paths and encodes
// them. Forward the full source URL at the transport boundary instead, so signed
// queries and encoded separators reach the server unchanged. HTTP status checks,
// redirects, response streaming, timeouts, and retries stay within OpenDAL.
struct UrlTransport {
    uri: Uri,
    limit: usize,
    inner: HttpTransporter,
}

impl HttpTransport for UrlTransport {
    async fn fetch(&self, mut request: Request<Buffer>) -> opendal::Result<Response<HttpBody>> {
        *request.uri_mut() = self.uri.clone();
        let response = self.inner.fetch(request).await?;
        // The HTTP service buffers error response bodies before classifying them.
        // Bound those too, including chunked errors without a content length.
        if response.status().is_success() {
            return Ok(response);
        }
        let limit = self.limit;
        let (parts, body) = response.into_parts();
        let body = body.map_inner(move |stream| {
            let mut buffered = 0;
            Box::new(stream.map(move |chunk| {
                let chunk = chunk?;
                if chunk.len() > limit - buffered {
                    return Err(opendal::Error::new(
                        opendal::ErrorKind::Unexpected,
                        "blob exceeds max_blob_bytes",
                    ));
                }
                buffered += chunk.len();
                Ok(chunk)
            }))
        });
        Ok(Response::from_parts(parts, body))
    }
}
