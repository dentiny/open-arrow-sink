//! Pluggable download I/O; concurrency control belongs to BlobDownloader.
use crate::{HttpConcurrencyLimitLayer, ObjectPath, Storage, StorageResolver, Url};
use bytes::Bytes;
use futures::stream::BoxStream;
use open_arrow_sink_core::{Error, Result, async_trait};
use std::sync::Arc;

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
    /// Wrap errors in [`SourceDownloadError`] to override their recovery scope.
    async fn open(&self, source: &str, context: DownloadContext) -> Result<DownloadStream>;
}

#[derive(Clone)]
pub struct DownloadContext {
    pub(crate) chunk_bytes: usize,
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

/// Whether a failed source download can be recovered independently of other rows.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum DownloadFailureScope {
    /// A known failure specific to a blob, eligible for SkipRow or Null.
    Object,
    /// Invalid input/configuration or storage authentication; never recover per row.
    Configuration,
    /// The failure could affect every object. Recovery must not hide it.
    Unknown,
}

/// An error opening or reading a source, distinct from local buffering errors.
#[derive(Debug)]
pub struct SourceDownloadError {
    scope: DownloadFailureScope,
    source: Error,
}
impl SourceDownloadError {
    /// Explicitly classify custom downloader errors; the download pool preserves
    /// this classification through wrappers and mid-stream failures.
    pub fn new(scope: DownloadFailureScope, source: Error) -> Self {
        Self { scope, source }
    }

    pub fn scope(&self) -> DownloadFailureScope {
        self.scope
    }

    /// Find the download error without making callers inspect backend error chains.
    pub fn find<'a>(mut error: &'a (dyn std::error::Error + 'static)) -> Option<&'a Self> {
        loop {
            if let Some(download) = error.downcast_ref::<Self>() {
                return Some(download);
            }
            error = error.source()?;
        }
    }

    pub(crate) fn wrap(source: &str, error: Error) -> Error {
        if Self::find(&error).is_some() {
            return error;
        }
        let scope = classify(source, &error);
        Error::backend(Self::new(scope, error))
    }
}
impl std::fmt::Display for SourceDownloadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.source.fmt(f)
    }
}
impl std::error::Error for SourceDownloadError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.source)
    }
}

fn classify(source: &str, mut error: &(dyn std::error::Error + 'static)) -> DownloadFailureScope {
    use DownloadFailureScope::{Configuration, Object, Unknown};
    let http = Url::parse(source).is_ok_and(|url| matches!(url.scheme(), "http" | "https"));
    loop {
        if error.is::<::url::ParseError>() {
            return Configuration;
        }
        if let Some(error) = error.downcast_ref::<opendal::Error>() {
            return match error.kind() {
                opendal::ErrorKind::NotFound => Object,
                opendal::ErrorKind::PermissionDenied if http => Object,
                opendal::ErrorKind::ConfigInvalid
                | opendal::ErrorKind::Unsupported
                | opendal::ErrorKind::PermissionDenied => Configuration,
                _ => Unknown,
            };
        }
        match error.downcast_ref::<Error>() {
            Some(Error::NotFound(_)) => return Object,
            Some(Error::InvalidConfig(_) | Error::InvalidInput(_) | Error::Unsupported(_)) => {
                return Configuration;
            }
            _ => {}
        }
        let Some(source) = error.source() else {
            return Unknown;
        };
        error = source;
    }
}
