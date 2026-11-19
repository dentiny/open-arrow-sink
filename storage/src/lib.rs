//! Shared OpenDAL byte storage, following open-arrow-access/storage.
//! Storage clones share a default budget of 64 ongoing HTTP requests.
//! Formats and dataset publication belong to sink crates.
mod backend;
mod config;
mod destination;
mod error;
mod http_limit;
mod io_metrics;
mod object_path;
mod resolver;
mod retry;
mod url;
mod validation;
pub use resolver::StorageResolver;

pub use ::url::Url;
pub use backend::{DirectoryState, Storage, StorageWriter};
pub use config::{RetryConfig, StorageConfig, TimeoutConfig};
pub use destination::DestinationResolver;
pub use http_limit::{HttpConcurrencyLimitLayer, HttpRequestPermit};
pub use io_metrics::IoMetricsLayer;
pub use object_path::ObjectPath;
pub use opendal;

mod object_store;

impl Storage {
    /// Adapt this namespace's operator to the object_store API. Multipart uploads
    /// share the byte writer's part-size and concurrency settings; other operations
    /// retain the upstream OpenDAL adapter semantics.
    pub fn object_store(&self) -> std::sync::Arc<dyn ::object_store::ObjectStore> {
        std::sync::Arc::new(object_store::UploadStore::new(self.clone()))
    }
}

mod downloader;
mod inline_source;
pub use downloader::{BlobDownload, BlobDownloader};

mod download_source;
mod opendal_downloader;
pub use download_source::{
    DownloadContext, DownloadStream, Downloader, ResolvedSource, SourceDownloadError,
};
pub use opendal_downloader::OpenDalDownloader;
