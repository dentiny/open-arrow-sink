use std::{
    num::NonZeroUsize,
    sync::{Arc, OnceLock},
};

use open_arrow_sink_core::{Error, Result};
use tokio::sync::Semaphore;

use crate::{
    error::backend,
    validation::{validate_blob_chunk, validate_blob_response, validate_blob_url},
};

/// Shared HTTP client and concurrency budget for blob downloads.
///
/// Cloning an `Arc<DownloadPool>` shares both connections and the download limit
/// across layers and writers. Downloads run on the caller's Tokio runtime and
/// are cancelled when the write future is dropped. No retries or request timeout
/// are configured by this pool.
#[derive(Debug)]
pub struct DownloadPool {
    client: reqwest::Client,
    permits: Semaphore,
    concurrency: NonZeroUsize,
}

impl DownloadPool {
    /// Create an independent pool with at most `concurrency` active downloads.
    /// A permit covers the entire response body, not just the response headers.
    pub fn new(concurrency: NonZeroUsize) -> Result<Self> {
        Self::with_client(
            concurrency,
            reqwest::Client::builder().build().map_err(backend)?,
        )
    }

    /// Use a caller-configured HTTP client (for example, with custom TLS or proxy
    /// settings). Any timeouts or retry policy on that client remain in effect.
    ///
    /// Returns an error if `concurrency` exceeds the semaphore's capacity.
    pub fn with_client(concurrency: NonZeroUsize, client: reqwest::Client) -> Result<Self> {
        if concurrency.get() > Semaphore::MAX_PERMITS {
            return Err(Error::Unsupported(
                "download concurrency exceeds semaphore capacity".into(),
            ));
        }
        Ok(Self {
            client,
            permits: Semaphore::new(concurrency.get()),
            concurrency,
        })
    }

    pub(crate) fn concurrency(&self) -> usize {
        self.concurrency.get()
    }

    pub(crate) fn global() -> Result<Arc<Self>> {
        static POOL: OnceLock<Arc<DownloadPool>> = OnceLock::new();
        if let Some(pool) = POOL.get() {
            return Ok(pool.clone());
        }
        let pool = Arc::new(Self::new(NonZeroUsize::new(16).unwrap())?);
        Ok(POOL.get_or_init(|| pool).clone())
    }

    pub(crate) async fn download(&self, url: &str, limit: usize) -> Result<Vec<u8>> {
        let url = validate_blob_url(url)?;
        // The semaphore is private and never closed. Dropping this future releases
        // its permit and cancels the request, including while reading the body.
        let _permit = self.permits.acquire().await.expect("pool is never closed");
        let mut response = self
            .client
            .get(url)
            .send()
            .await
            .map_err(|e| backend(e.without_url()))?
            .error_for_status()
            .map_err(|e| backend(e.without_url()))?;
        validate_blob_response(&response, limit)?;
        let mut bytes = Vec::new();
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|e| backend(e.without_url()))?
        {
            validate_blob_chunk(bytes.len(), chunk.len(), limit)?;
            bytes.extend_from_slice(&chunk);
        }
        Ok(bytes)
    }
}
