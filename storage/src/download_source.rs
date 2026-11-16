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
}
impl DownloadContext {
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
        if let Some(storage) = &self.primary {
            if let Ok(path) = storage.resolve(url) {
                return Ok((storage.clone(), path));
            }
        }
        let storage = self.sources.resolve(url)?;
        let path = storage.resolve(url)?;
        Ok((storage, path))
    }
}
