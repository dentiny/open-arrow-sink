//! Shared OpenDAL byte storage, following open-arrow-access/storage.
//! Storage clones share a default budget of 64 ongoing HTTP requests.
//! Formats and dataset publication belong to sink crates.
mod backend;
mod config;
mod error;
mod http_limit;
mod validation;

pub use backend::{Storage, StorageWriter};
pub use config::{RetryConfig, StorageConfig, TimeoutConfig};
pub use http_limit::HttpConcurrencyLimitLayer;
pub use opendal;
pub use url::Url;

#[cfg(feature = "http")]
mod download_pool;
#[cfg(feature = "http")]
pub use download_pool::DownloadPool;
