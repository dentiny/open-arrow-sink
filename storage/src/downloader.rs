use std::{num::NonZeroUsize, sync::Arc};

use futures::{TryStreamExt, stream::BoxStream};
use http::{Request, Response, Uri};
use open_arrow_sink_core::{BlobChunk, Error, Result};
use opendal::{Buffer, HttpBody, HttpTransport, HttpTransporter, Operator, services::Http};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::{HttpConcurrencyLimitLayer, Storage, StorageConfig, error, validation};

// Placeholder namespace for HTTP storage; requests use each complete source URL.
const HTTP_PLACEHOLDER: &str = "https://download.invalid/";

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
#[derive(Clone, Debug)]
pub struct BlobDownloader {
    http: Storage,
    sources: crate::StorageResolver,
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
    /// HTTP(S) downloads use a default `http_storage` whose request budget
    /// equals `concurrency`; replace it with `with_http_storage`.
    pub fn new(concurrency: NonZeroUsize, max_buffered_bytes: NonZeroUsize) -> Result<Self> {
        validation::validate_http_concurrency(concurrency.get())?;
        validation::validate_download_limit(max_buffered_bytes)?;
        let reservation = max_buffered_bytes.get() / concurrency.get();
        let chunk_bytes = CHUNK_BYTES.min(reservation / 3).max(1);
        let download_bytes = reservation.saturating_sub(chunk_bytes) / 2;
        Ok(Self {
            download_bytes,
            chunk_bytes,
            http: Self::http_storage(StorageConfig::default())?
                .with_http_concurrency_limit(HttpConcurrencyLimitLayer::new(concurrency.get())?),
            sources: crate::StorageResolver::default(),
            stream_slots: Arc::new(Semaphore::new(concurrency.get())),
            concurrency,
            buffers: Arc::new(Semaphore::new(max_buffered_bytes.get())),
            admission: Arc::default(),
            max_buffered_bytes,
        })
    }

    /// Build storage for HTTP(S) sources with the given retry/timeout policies.
    /// Configure its request budget and transport like any other `Storage`.
    pub fn http_storage(config: StorageConfig) -> Result<Storage> {
        opendal::install_default();
        let endpoint = HTTP_PLACEHOLDER.trim_end_matches('/');
        let operator = Operator::new(Http::default().endpoint(endpoint)).map_err(Error::backend)?;
        Storage::try_new(
            operator,
            HTTP_PLACEHOLDER.parse().map_err(Error::backend)?,
            config,
        )
    }

    /// Route HTTP(S) downloads through `storage`, keeping its policies, request
    /// budget, and transport. Build it with `http_storage`.
    pub fn with_http_storage(mut self, storage: Storage) -> Result<Self> {
        if storage.operator().info().scheme() != "http" {
            return Err(error::http_storage_service());
        }
        self.http = storage;
        Ok(self)
    }

    /// Register a source store for non-HTTP downloads, including custom S3 credentials.
    pub fn with_source_storage(mut self, storage: crate::Storage) -> Self {
        self.sources = self.sources.with_storage(storage);
        self
    }

    /// Add source namespaces while retaining this downloader's shared budgets.
    pub fn with_source_resolver(mut self, sources: &crate::StorageResolver) -> Self {
        self.sources = self.sources.merged(sources);
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

    /// Maximum combined payload bytes returned by one `download_all` call.
    pub fn max_download_bytes(&self) -> usize {
        self.download_bytes
    }

    /// Maximum application copy chunk used by full-blob downloads.
    pub fn chunk_bytes(&self) -> usize {
        self.chunk_bytes
    }

    /// Configure full-download payload and chunk caps. Each download group
    /// atomically reserves `2 * payload_bytes + chunk_bytes` from the shared
    /// budget before reading: payloads, allocation/copy headroom, and scratch.
    /// A group's selected blobs must fit together; limits do not grow with data.
    pub fn with_download_limits(
        mut self,
        payload_bytes: NonZeroUsize,
        chunk_bytes: NonZeroUsize,
    ) -> Result<Self> {
        let reservation = payload_bytes
            .get()
            .checked_mul(2)
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
                self.download_bytes * 2 + self.chunk_bytes,
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

    async fn reader(&self, source: &str) -> Result<opendal::Reader> {
        let url = crate::Url::parse(source).map_err(Error::backend)?;
        let (operator, path) = if matches!(url.scheme(), "http" | "https") {
            let uri = validation::validate_download_url(source)?;
            let operator = self.http.operator();
            let context = operator.base_context();
            let transport = HttpTransporter::new(UrlTransport {
                uri,
                inner: context.http_transport().clone(),
            });
            let context = context.with_http_transport(transport);
            (operator.with_context(context), "blob".to_owned())
        } else {
            let storage = self.sources.resolve(&url)?;
            (storage.operator(), storage.path(&url)?)
        };
        // HTTP sources need not implement HEAD or range requests. Keep one
        // streaming GET; chunk_bytes bounds the application-side copy below.
        operator.reader(&path).await.map_err(Error::backend)
    }

    /// Open a pull-based download. Dropping the stream cancels pending I/O and
    /// releases its stream slot; already yielded chunks retain their reservations.
    pub async fn stream(&self, source: &str) -> Result<BoxStream<'static, Result<BlobChunk>>> {
        let stream_slot = self
            .stream_slots
            .clone()
            .acquire_owned()
            .await
            .expect("pool is never closed");
        let reader = self
            .reader(source)
            .await?
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
        Ok(error::download_response(response))
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
            .reader(source)
            .await?
            .into_stream(..)
            .await
            .map_err(Error::backend)?;
        let mut data = Vec::new();
        while let Some(mut buffer) = reader.try_next().await.map_err(Error::backend)? {
            while !buffer.is_empty() {
                let size = self.downloader.chunk_bytes.min(buffer.len());
                if let Some(metrics) = metrics {
                    metrics.record_read(size as u64);
                }
                for part in buffer.split_to(size) {
                    self.append(&mut data, &part)?;
                }
            }
        }
        Ok(self.finish(data))
    }
}
