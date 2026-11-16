use std::ops::Deref;

pub use lance_file::version::LanceFileVersion;
use open_arrow_sink_core::{BlobHandleConfig, IndexSpec, WriteOptions};

/// Lance-specific options. Configure these on `LanceDatasetSink` before layering.
#[derive(Debug, Clone, Default)]
pub struct LanceOptions {
    pub indexes: Vec<IndexSpec>,
    /// None keeps Lance's default; supported versions are V2_2 and V2_3.
    pub storage_version: Option<LanceFileVersion>,
    /// None uses the native defaults, including ingestion of external URIs.
    pub blob_handle: Option<BlobHandleConfig>,
}

#[derive(Debug, Clone)]
pub(crate) struct SessionOptions {
    pub shared: WriteOptions,
    pub indexes: Vec<IndexSpec>,
    pub storage_version: Option<LanceFileVersion>,
    pub blob_handle: Option<BlobHandleConfig>,
}
impl Deref for SessionOptions {
    type Target = WriteOptions;
    fn deref(&self) -> &WriteOptions {
        &self.shared
    }
}
impl LanceOptions {
    pub(crate) fn session(&self, shared: WriteOptions) -> SessionOptions {
        SessionOptions {
            shared,
            indexes: self.indexes.clone(),
            storage_version: self.storage_version,
            blob_handle: self.blob_handle.clone(),
        }
    }
}
