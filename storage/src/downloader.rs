use std::{num::NonZeroUsize, sync::Arc};

use futures::TryStreamExt;
use open_arrow_sink_core::{Error, Result};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::{
    DownloadContext, DownloadStream, Downloader, HttpConcurrencyLimitLayer, OpenDalDownloader,
    Storage, StorageConfig, validation,
};

// Default maximum number of active download streams in one pool.
const DEFAULT_CONCURRENCY: NonZeroUsize = NonZeroUsize::new(64).unwrap();
// Application read chunk size; this does not limit complete payloads.
const CHUNK_BYTES: usize = 2 * 1024 * 1024;

/// Blob materialization with shared download concurrency control.
/// Clones share admission slots. Complete payloads have no configured byte limit
/// and are buffered in memory before being passed to the destination SDK.
#[derive(Clone)]
pub struct BlobDownloader {
    backend: Arc<dyn Downloader>,
    http_limit: HttpConcurrencyLimitLayer,
    sources: Arc<crate::StorageResolver>,
    primary: Option<Storage>,
    // Bound active groups, including non-HTTP sources, until the group is dropped.
    stream_slots: Arc<Semaphore>,
    concurrency: NonZeroUsize,
    chunk_bytes: usize,
    io_request_size: Option<NonZeroUsize>,
}

impl BlobDownloader {
    /// Create an independent download pool with the given concurrency.
    /// The default OpenDAL provider uses the shared HTTP request budget.
    /// Replace download I/O with `with_downloader`.
    pub fn new(concurrency: NonZeroUsize) -> Result<Self> {
        validation::validate_http_concurrency(concurrency.get())?;
        Ok(Self {
            chunk_bytes: CHUNK_BYTES,
            io_request_size: None,
            backend: Arc::new(OpenDalDownloader::new(StorageConfig::default())?),
            http_limit: HttpConcurrencyLimitLayer::new(concurrency.get())?,
            sources: Arc::default(),
            primary: None,
            stream_slots: Arc::new(Semaphore::new(concurrency.get())),
            concurrency,
        })
    }

    /// Set the storage read request chunk target, independently of output chunks.
    /// The default OpenDAL downloader uses bounded range reads when configured.
    /// Small objects and final ranges can be smaller; this is not a payload limit.
    pub fn with_io_request_size(mut self, io_request_size: NonZeroUsize) -> Self {
        self.io_request_size = Some(io_request_size);
        self
    }

    /// Replace download I/O without replacing concurrency control.
    pub fn with_downloader(mut self, backend: Arc<dyn Downloader>) -> Self {
        self.backend = backend;
        self
    }
    /// Share a request budget with other downloaders or destination stores.
    pub fn with_http_concurrency_limit(mut self, limit: HttpConcurrencyLimitLayer) -> Self {
        self.http_limit = limit;
        self
    }

    /// Register a source store, including HTTP authentication and custom S3 credentials.
    pub fn with_source_storage(mut self, storage: crate::Storage) -> Self {
        self.sources = Arc::new(self.sources.as_ref().clone().with_storage(storage));
        self
    }

    /// Source namespaces retained when this downloader is installed on a sink.
    pub fn source_resolver(&self) -> Arc<crate::StorageResolver> {
        self.sources.clone()
    }

    /// Replace source namespaces and share their URL-derived cache while retaining
    /// this downloader's concurrency limits.
    pub fn with_source_resolver(mut self, sources: Arc<crate::StorageResolver>) -> Self {
        self.sources = sources;
        self
    }

    /// Prefer this operator within its own namespace. Backend adapters bind
    /// destination-owned sources here without changing a shared source resolver.
    pub fn with_primary_storage(mut self, storage: Storage) -> Self {
        self.primary = Some(storage);
        self
    }

    pub fn concurrency(&self) -> usize {
        self.concurrency.get()
    }
    /// Bound queued and active groups.
    pub fn in_flight_limit(&self) -> usize {
        self.concurrency()
    }

    /// Create an independent pool with 64 stream slots.
    /// Share it explicitly with `Arc` when needed.
    pub fn with_defaults() -> Result<Self> {
        Self::new(DEFAULT_CONCURRENCY)
    }

    /// Application read chunk size, independent of complete payload size.
    pub fn chunk_bytes(&self) -> usize {
        self.chunk_bytes
    }

    /// Download one complete blob without a configured payload byte limit.
    pub async fn download(&self, source: &str) -> Result<bytes::Bytes> {
        Ok(self.download_all(&[source], None).await?.remove(0))
    }

    /// Download a group of complete blobs without a configured payload byte limit.
    /// Sources in a group are read sequentially; independent groups can run
    /// concurrently up to `concurrency()`.
    pub async fn download_all(
        &self,
        sources: &[&str],
        metrics: Option<&open_arrow_sink_core::IoObservers>,
    ) -> Result<Vec<bytes::Bytes>> {
        if sources.is_empty() {
            return Ok(Vec::new());
        }
        let mut group = self.begin().await?;
        let mut blobs = Vec::with_capacity(sources.len());
        for source in sources {
            blobs.push(group.download(source, metrics).await?);
        }
        Ok(blobs)
    }

    /// Admit a group for downloading or buffering existing blob data.
    /// The slot is released when the group is dropped, regardless of payload owners.
    pub async fn begin(&self) -> Result<BlobDownload> {
        let slot = self
            .stream_slots
            .clone()
            .acquire_owned()
            .await
            .expect("pool is never closed");
        Ok(BlobDownload {
            downloader: self.clone(),
            _slot: slot,
        })
    }

    fn context(
        &self,
        chunk_bytes: usize,
        metrics: Option<&open_arrow_sink_core::IoObservers>,
    ) -> DownloadContext {
        DownloadContext {
            chunk_bytes,
            io_request_size: self.io_request_size,
            http_limit: self.http_limit.clone(),
            sources: self.sources.clone(),
            primary: self.primary.clone(),
            io_metrics: metrics.cloned(),
        }
    }
    async fn open(
        &self,
        source: &str,
        chunk_bytes: usize,
        metrics: Option<&open_arrow_sink_core::IoObservers>,
    ) -> Result<DownloadStream> {
        self.backend
            .open(source, self.context(chunk_bytes, metrics))
            .await
    }
}

/// One admitted group of complete blob payloads. Dropping it cancels downloads
/// and releases its concurrency slot; returned bytes remain independently owned.
pub struct BlobDownload {
    downloader: BlobDownloader,
    _slot: OwnedSemaphorePermit,
}
impl BlobDownload {
    fn append(data: &mut Vec<u8>, chunk: &[u8]) -> Result<()> {
        data.try_reserve(chunk.len()).map_err(Error::backend)?;
        data.extend_from_slice(chunk);
        Ok(())
    }
    /// Copy existing blob bytes without a configured payload byte limit.
    pub fn buffer(&mut self, source: &[u8]) -> Result<bytes::Bytes> {
        let mut data = Vec::new();
        Self::append(&mut data, source)?;
        Ok(data.into())
    }
    /// Return an assembled payload buffer.
    pub fn finish_buffer(&self, data: Vec<u8>) -> Result<bytes::Bytes> {
        Ok(data.into())
    }

    pub async fn download(
        &mut self,
        source: &str,
        metrics: Option<&open_arrow_sink_core::IoObservers>,
    ) -> Result<bytes::Bytes> {
        let source_error = |error| Error::backend(crate::SourceDownloadError(error));
        let mut reader = self
            .downloader
            .open(source, self.downloader.chunk_bytes, metrics)
            .await
            .map_err(source_error)?;
        let mut data = Vec::new();
        loop {
            let chunk = match reader.try_next().await {
                Ok(Some(chunk)) => chunk,
                Ok(None) => break,
                Err(error) => {
                    return Err(source_error(error));
                }
            };
            validate_chunk(&chunk, self.downloader.chunk_bytes)?;
            if let Some(metrics) = metrics {
                metrics.record_read(chunk.len() as u64);
            }
            Self::append(&mut data, &chunk)?;
        }
        Ok(data.into())
    }
}

fn validate_chunk(chunk: &[u8], limit: usize) -> Result<()> {
    if chunk.len() > limit {
        return Err(Error::InvalidInput(format!(
            "downloader returned a {}-byte chunk exceeding the {limit}-byte limit",
            chunk.len()
        )));
    }
    Ok(())
}
impl std::fmt::Debug for BlobDownloader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BlobDownloader")
            .field("concurrency", &self.concurrency)
            .field("chunk_bytes", &self.chunk_bytes)
            .field("io_request_size", &self.io_request_size)
            .finish_non_exhaustive()
    }
}
