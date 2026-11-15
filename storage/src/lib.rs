//! Shared byte storage. Formats and dataset publication belong to sink crates.
mod config;
mod storage;

pub use config::{RetryConfig, StorageConfig, TimeoutConfig};
pub use opendal;
pub use storage::{Storage, StorageWriter};
pub use url::Url;
