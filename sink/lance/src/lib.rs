//! Versioned Lance datasets over shared OpenDAL storage. Requires a Tokio runtime.

mod blob;
mod blob_writer;
mod committer;
mod compression;
mod error;
mod provider;
mod sink;
mod validation;

pub use lance::dataset::ExternalBlobMode;
pub use sink::LanceDatasetSink;

mod index;

mod parallel;

mod storage;
pub use open_arrow_sink_storage::{Storage, Url};
