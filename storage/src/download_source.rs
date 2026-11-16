//! Pluggable download I/O; scheduling and payload budgets belong to BlobDownloader.
use crate::{HttpConcurrencyLimitLayer, ObjectPath, Storage, StorageResolver, Url};
use bytes::Bytes;
use futures::stream::BoxStream;
use open_arrow_sink_core::{Result, async_trait};
use std::sync::Arc;

pub type DownloadStream = BoxStream<'static, Result<Bytes>>;

/// Download I/O for custom protocols, authentication, or clients.
/// Return chunks no larger than context.chunk_bytes(). Keep HTTP permits until
/// response EOF/drop; dropping the stream must cancel its pending I/O.
/// BlobDownloader applies admission, payload limits, and accounting to every impl.
#[async_trait]
pub trait Downloader: Send + Sync {
    async fn open(&self, source: &str, context: DownloadContext) -> Result<DownloadStream>;
}

#[derive(Clone)]
pub struct DownloadContext {
    pub(crate) chunk_bytes: usize,
    pub(crate) http_limit: HttpConcurrencyLimitLayer,
    pub(crate) sources: Arc<StorageResolver>,
    pub(crate) primary: Option<Storage>,
    pub(crate) io_metrics: Option<open_arrow_sink_core::IoMetrics>,
}
impl DownloadContext {
    // The wrapper records consumed chunks; source operators retain only observers
    // not already present on that wrapper, including partially overlapping handles.
    pub(crate) fn unobserved_metrics(
        &self,
        metrics: Option<open_arrow_sink_core::IoMetrics>,
    ) -> Option<open_arrow_sink_core::IoMetrics> {
        metrics.and_then(|metrics| match &self.io_metrics {
            Some(observed) => metrics.excluding(observed),
            None => Some(metrics),
        })
    }
    fn source_storage(&self, storage: Storage) -> Storage {
        let (storage, metrics) = storage.without_io_metrics();
        match self.unobserved_metrics(metrics) {
            Some(metrics) => storage.with_io_metrics(metrics),
            None => storage,
        }
    }
    pub fn chunk_bytes(&self) -> usize {
        self.chunk_bytes
    }
    /// Custom HTTP clients acquire once per request and keep the guard through
    /// the response body. OpenDAL transports can use wrap() instead.
    pub fn http_requests(&self) -> &HttpConcurrencyLimitLayer {
        &self.http_limit
    }
    /// Resolve a registered source, preferring a backend's own namespace.
    pub fn resolve_source(&self, url: &Url) -> Result<(Storage, ObjectPath)> {
        if let Some(storage) = &self.primary
            && let Ok(path) = storage.resolve(url)
        {
            return Ok((self.source_storage(storage.clone()), path));
        }
        let storage = self.sources.resolve(url)?;
        let path = storage.resolve(url)?;
        Ok((self.source_storage(storage), path))
    }
}

/// An error opening or reading a source, distinct from local memory-budget errors.
#[derive(Debug)]
pub struct SourceDownloadError(pub(crate) open_arrow_sink_core::Error);
impl std::fmt::Display for SourceDownloadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}
impl std::error::Error for SourceDownloadError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.0)
    }
}
