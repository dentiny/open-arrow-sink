//! Pluggable download I/O; concurrency control belongs to BlobDownloader.
use crate::{HttpConcurrencyLimitLayer, ObjectPath, Storage, StorageResolver, Url};
use bytes::Bytes;
use futures::stream::BoxStream;
use open_arrow_sink_core::{Result, async_trait};
use std::{num::NonZeroUsize, sync::Arc};

/// A source namespace and the object path resolved within it.
#[derive(Clone, Debug)]
pub struct ResolvedSource {
    pub storage: Storage,
    pub path: ObjectPath,
}

pub type DownloadStream = BoxStream<'static, Result<Bytes>>;

/// Download I/O for custom protocols, authentication, or clients.
/// Return chunks no larger than context.chunk_bytes(). Keep HTTP permits until
/// response EOF/drop; dropping the stream must cancel its pending I/O.
/// BlobDownloader applies admission and accounting to every impl.
#[async_trait]
pub trait Downloader: Send + Sync {
    /// Open `source` and return a stream of payload chunks in source order.
    /// Use `context` to resolve storage and share HTTP concurrency limits; chunks
    /// must not exceed [`DownloadContext::chunk_bytes`]. Keep request permits
    /// until EOF or stream drop, and cancel pending I/O when the stream is dropped.
    /// Return opening failures here and subsequent read failures through the stream.
    async fn open(&self, source: &str, context: DownloadContext) -> Result<DownloadStream>;
}

#[derive(Clone)]
pub struct DownloadContext {
    pub(crate) chunk_bytes: usize,
    pub(crate) io_request_size: Option<NonZeroUsize>,
    pub(crate) http_limit: HttpConcurrencyLimitLayer,
    pub(crate) sources: Arc<StorageResolver>,
    pub(crate) primary: Option<Storage>,
    pub(crate) io_metrics: Option<open_arrow_sink_core::IoObservers>,
}
impl DownloadContext {
    // The wrapper records consumed chunks; source operators retain only observers
    // not already present on that wrapper, including partially overlapping handles.
    pub(crate) fn unobserved_metrics(
        &self,
        metrics: Option<open_arrow_sink_core::IoObservers>,
    ) -> Option<open_arrow_sink_core::IoObservers> {
        metrics.and_then(|metrics| match &self.io_metrics {
            Some(observed) => metrics.excluding(observed),
            None => Some(metrics),
        })
    }
    fn source_storage(&self, storage: Storage) -> Storage {
        let crate::backend::UnobservedStorage { storage, observers } = storage.without_io_metrics();
        match self.unobserved_metrics(observers) {
            Some(metrics) => storage.with_io_metrics(metrics),
            None => storage,
        }
    }
    pub fn chunk_bytes(&self) -> usize {
        self.chunk_bytes
    }
    /// Optional storage I/O chunk target; independent of yielded chunk sizes.
    /// Custom downloaders should apply this to their range requests when supported.
    pub fn io_request_size(&self) -> Option<NonZeroUsize> {
        self.io_request_size
    }
    /// Custom HTTP clients acquire once per request and keep the guard through
    /// the response body. OpenDAL transports can use wrap() instead.
    pub fn http_requests(&self) -> &HttpConcurrencyLimitLayer {
        &self.http_limit
    }
    /// Bind storage for this source open without changing the enclosing downloader.
    ///
    /// Replaces destination/source namespaces for this context only. The default
    /// OpenDAL downloader retains its HTTP dispatch, chunking, metrics and request
    /// admission; custom credential resolvers need not duplicate those paths.
    /// Construction performs no I/O and retains the current metrics/request budget.
    pub fn with_source_storage(mut self, storage: Storage) -> Self {
        self.primary = Some(storage.clone());
        self.sources = Arc::new(StorageResolver::default().with_storage(storage));
        self
    }

    /// Resolve a registered source, preferring a backend's own namespace.
    pub fn resolve_source(&self, url: &Url) -> Result<ResolvedSource> {
        if let Some(storage) = &self.primary
            && let Ok(path) = storage.path(url)
        {
            return Ok(ResolvedSource {
                storage: self.source_storage(storage.clone()),
                path,
            });
        }
        let storage = self.sources.storage_for(url)?;
        let path = storage.path(url)?;
        Ok(ResolvedSource {
            storage: self.source_storage(storage),
            path,
        })
    }
}

/// An error opening or reading a source, distinct from local buffering errors.
#[derive(Debug)]
pub struct SourceDownloadError {
    pub(crate) url: String,
    pub(crate) error: open_arrow_sink_core::Error,
}
impl SourceDownloadError {
    pub fn url(&self) -> &str {
        &self.url
    }

    pub fn error(&self) -> &open_arrow_sink_core::Error {
        &self.error
    }

    /// Native OpenDAL HTTP/S3 404s and missing filesystem objects use NotFound.
    /// Custom downloaders must return a typed NotFound to opt into this policy.
    pub fn is_not_found(&self) -> bool {
        matches!(&self.error, open_arrow_sink_core::Error::NotFound(_))
    }
}
impl std::fmt::Display for SourceDownloadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "source {:?}: {}", self.url, self.error)
    }
}
impl std::error::Error for SourceDownloadError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.error)
    }
}
