use std::{num::NonZeroUsize, sync::Arc};

use futures::TryStreamExt;
use open_arrow_sink_core::{Error, Result};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::{
    DownloadContext, DownloadStream, Downloader, HttpConcurrencyLimitLayer, OpenDalDownloader,
    Storage, StorageConfig, error, validation,
};

// Default maximum number of active download streams in one pool.
const DEFAULT_CONCURRENCY: NonZeroUsize = NonZeroUsize::new(64).unwrap();
// Default shared transfer-buffer budget: 8 GiB, including consumer-held chunks.
// Excludes OpenDAL/HTTP/TLS buffers and destination writer buffers.
const DEFAULT_BUFFERED_BYTES: NonZeroUsize = NonZeroUsize::new(8 * 1024 * 1024 * 1024).unwrap();
// Maximum buffer allocated for each read: 2 MiB, capped by the pool budget.
const CHUNK_BYTES: usize = 2 * 1024 * 1024;

/// Blob materialization with shared download, chunk, and application-memory caps.
/// Clones share admission and byte budgets. Full payloads keep reservations through
/// their last owner, including Arrow buffers queued for SDK consumption.
/// OpenDAL/HTTP/TLS and native encoding/output caches have separate limits.
#[derive(Clone)]
pub struct BlobDownloader {
    backend: Arc<dyn Downloader>,
    http_limit: HttpConcurrencyLimitLayer,
    sources: Arc<crate::StorageResolver>,
    primary: Option<Storage>,
    // Bound lazy streams, including non-HTTP sources, until cancellation or EOF.
    stream_slots: Arc<Semaphore>,
    concurrency: NonZeroUsize,
    buffers: Arc<Semaphore>,
    admission: Arc<tokio::sync::Mutex<()>>,
    max_buffered_bytes: NonZeroUsize,
    download_bytes: usize,
    chunk_bytes: usize,
}

impl BlobDownloader {
    /// Create independent stream and transfer-buffer budgets. Every call honors
    /// `concurrency`; no process-wide state or environment configuration is used.
    /// The default OpenDAL provider uses the shared HTTP request budget.
    /// Replace download I/O with `with_downloader`.
    pub fn new(concurrency: NonZeroUsize, max_buffered_bytes: NonZeroUsize) -> Result<Self> {
        validation::validate_http_concurrency(concurrency.get())?;
        validation::validate_download_limit(max_buffered_bytes)?;
        let reservation = max_buffered_bytes.get() / concurrency.get();
        let chunk_bytes = CHUNK_BYTES.min(reservation / 3).max(1);
        let download_bytes = reservation.saturating_sub(chunk_bytes) / 3;
        Ok(Self {
            download_bytes,
            chunk_bytes,
            backend: Arc::new(OpenDalDownloader::new(StorageConfig::default())?),
            http_limit: HttpConcurrencyLimitLayer::new(concurrency.get())?,
            sources: Arc::default(),
            primary: None,
            stream_slots: Arc::new(Semaphore::new(concurrency.get())),
            concurrency,
            buffers: Arc::new(Semaphore::new(max_buffered_bytes.get())),
            admission: Arc::default(),
            max_buffered_bytes,
        })
    }

    /// Replace download I/O without replacing scheduling or memory accounting.
    pub fn with_downloader(mut self, backend: Arc<dyn Downloader>) -> Self {
        self.backend = backend;
        self
    }
    /// Share a request budget with other downloaders or destination stores.
    pub fn with_http_concurrency_limit(mut self, limit: HttpConcurrencyLimitLayer) -> Self {
        self.http_limit = limit;
        self
    }

    /// Register a source store for non-HTTP downloads, including custom S3 credentials.
    pub fn with_source_storage(mut self, storage: crate::Storage) -> Self {
        self.sources = Arc::new(self.sources.as_ref().clone().with_storage(storage));
        self
    }

    /// Replace source namespaces and share their URL-derived cache while retaining
    /// this downloader's transfer budgets.
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
    /// Bound queued and active row metadata as well as payload allocations.
    pub fn in_flight_limit(&self) -> usize {
        self.concurrency()
            .min(self.max_buffered_bytes() / (3 * self.download_bytes + self.chunk_bytes))
            .max(1)
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

    /// Maximum combined payload bytes returned by one `download_all` call.
    pub fn max_download_bytes(&self) -> usize {
        self.download_bytes
    }

    /// Maximum application copy chunk used by full-blob downloads.
    pub fn chunk_bytes(&self) -> usize {
        self.chunk_bytes
    }

    /// Configure full-download payload and chunk caps. Each download group
    /// atomically reserves `3 * payload_bytes + chunk_bytes` from the shared
    /// budget before reading: payloads, assembly/SDK copy headroom, and scratch.
    /// A group's selected blobs must fit together; limits do not grow with data.
    pub fn with_download_limits(
        mut self,
        payload_bytes: NonZeroUsize,
        chunk_bytes: NonZeroUsize,
    ) -> Result<Self> {
        let reservation = payload_bytes
            .get()
            .checked_mul(3)
            .and_then(|bytes| bytes.checked_add(chunk_bytes.get()));
        if reservation.is_none_or(|bytes| bytes > self.max_buffered_bytes.get()) {
            return Err(error::download_payload_limit());
        }
        self.download_bytes = payload_bytes.get();
        self.chunk_bytes = chunk_bytes.get();
        Ok(self)
    }

    /// Download one complete blob. Its returned bytes retain the memory budget
    /// until the last clone or Arrow buffer sharing them is dropped.
    pub async fn download(&self, source: &str) -> Result<bytes::Bytes> {
        Ok(self.download_all(&[source], None).await?.remove(0))
    }

    /// Download a group of complete blobs under one atomic memory reservation.
    /// Returned Bytes retain that reservation through clones and Arrow buffers,
    /// including while queued at the sink. Oversized groups fail, without staging
    /// or publishing partial blobs. Sources in a group are read sequentially;
    /// callers can run independent groups concurrently up to `concurrency()`.
    pub async fn download_all(
        &self,
        sources: &[&str],
        metrics: Option<&open_arrow_sink_core::IoMetrics>,
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

    /// Reserve a bounded group for downloading or buffering existing blob data.
    /// Group admission is atomic; returned payloads retain its memory reservation.
    pub async fn begin(&self) -> Result<BlobDownload> {
        if self.download_bytes == 0 {
            return Err(error::download_payload_limit());
        }
        let slot = self
            .stream_slots
            .clone()
            .acquire_owned()
            .await
            .expect("pool is never closed");
        let reservation = Arc::new(
            acquire_bytes(
                self.buffers.clone(),
                self.admission.clone(),
                self.download_bytes * 3 + self.chunk_bytes,
            )
            .await,
        );
        Ok(BlobDownload {
            downloader: self.clone(),
            _slot: slot,
            reservation,
            total: 0,
        })
    }

    fn context(&self, chunk_bytes: usize) -> DownloadContext {
        DownloadContext {
            chunk_bytes,
            http_limit: self.http_limit.clone(),
            sources: self.sources.clone(),
            primary: self.primary.clone(),
        }
    }
    async fn open(&self, source: &str, chunk_bytes: usize) -> Result<DownloadStream> {
        self.backend.open(source, self.context(chunk_bytes)).await
    }
}

// Serialize full reservations so groups larger than u32::MAX can acquire and
// merge Tokio permits without two groups each holding an incomplete allocation.
// No payload I/O starts until the complete reservation has been acquired.
async fn acquire_bytes(
    buffers: Arc<Semaphore>,
    admission: Arc<tokio::sync::Mutex<()>>,
    bytes: usize,
) -> OwnedSemaphorePermit {
    let _admission = admission.lock().await;
    let first = bytes.min(u32::MAX as usize);
    let mut reservation = buffers
        .clone()
        .acquire_many_owned(first as u32)
        .await
        .expect("pool is never closed");
    let mut remaining = bytes - first;
    while remaining > 0 {
        let next = remaining.min(u32::MAX as usize);
        reservation.merge(
            buffers
                .clone()
                .acquire_many_owned(next as u32)
                .await
                .expect("pool is never closed"),
        );
        remaining -= next;
    }
    reservation
}

struct DownloadedBytes {
    data: Vec<u8>,
    _reservation: Arc<OwnedSemaphorePermit>,
}
impl AsRef<[u8]> for DownloadedBytes {
    fn as_ref(&self) -> &[u8] {
        &self.data
    }
}

/// One admitted group of complete blob payloads. Dropping it cancels downloads;
/// bytes already returned retain the memory reservation independently.
pub struct BlobDownload {
    downloader: BlobDownloader,
    _slot: OwnedSemaphorePermit,
    reservation: Arc<OwnedSemaphorePermit>,
    total: usize,
}
impl BlobDownload {
    fn append(&mut self, data: &mut Vec<u8>, chunk: &[u8]) -> Result<()> {
        if chunk.len() > self.downloader.download_bytes - self.total {
            return Err(error::download_too_large(self.downloader.download_bytes));
        }
        data.try_reserve_exact(chunk.len())
            .map_err(Error::backend)?;
        data.extend_from_slice(chunk);
        self.total += chunk.len();
        Ok(())
    }
    fn finish(&self, data: Vec<u8>) -> bytes::Bytes {
        bytes::Bytes::from_owner(DownloadedBytes {
            data,
            _reservation: self.reservation.clone(),
        })
    }
    /// Copy existing blob bytes under the same admission and memory limits.
    pub fn buffer(&mut self, source: &[u8]) -> Result<bytes::Bytes> {
        let mut data = Vec::new();
        self.append(&mut data, source)?;
        Ok(self.finish(data))
    }
    /// Retain this group's reservation on a buffer assembled from its payloads.
    /// Drop the intermediate payloads while assembling to stay within copy headroom.
    pub fn finish_buffer(&self, data: Vec<u8>) -> Result<bytes::Bytes> {
        if data.len() > self.total {
            return Err(error::download_too_large(self.total));
        }
        Ok(self.finish(data))
    }

    pub async fn download(
        &mut self,
        source: &str,
        metrics: Option<&open_arrow_sink_core::IoMetrics>,
    ) -> Result<bytes::Bytes> {
        let mut reader = self
            .downloader
            .open(source, self.downloader.chunk_bytes)
            .await?;
        let mut data = Vec::new();
        while let Some(chunk) = reader.try_next().await? {
            validate_chunk(&chunk, self.downloader.chunk_bytes)?;
            if let Some(metrics) = metrics {
                metrics.record_read(chunk.len() as u64);
            }
            self.append(&mut data, &chunk)?;
        }
        Ok(self.finish(data))
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
            .field("max_buffered_bytes", &self.max_buffered_bytes)
            .field("max_download_bytes", &self.download_bytes)
            .field("chunk_bytes", &self.chunk_bytes)
            .finish_non_exhaustive()
    }
}
