//! Shared OpenDAL byte storage, following open-arrow-access/storage.
//! Storage clones share a default budget of 64 ongoing HTTP requests.
//! Formats and dataset publication belong to sink crates.
mod backend;
mod config;
mod error;
mod http_limit;
mod resolver;
mod validation;
pub use resolver::StorageResolver;

pub use backend::{DirectoryState, Storage, StorageWriter};
pub use config::{RetryConfig, StorageConfig, TimeoutConfig};
pub use http_limit::HttpConcurrencyLimitLayer;
pub use opendal;
pub use url::Url;

mod object_store;

impl Storage {
    /// Adapt this namespace's operator to the object_store API. Multipart uploads
    /// share the byte writer's part-size and concurrency settings; other operations
    /// retain the upstream OpenDAL adapter semantics.
    pub fn object_store(&self) -> std::sync::Arc<dyn ::object_store::ObjectStore> {
        std::sync::Arc::new(object_store::UploadStore::new(self.clone()))
    }
}
