//! Versioned Lance datasets over shared OpenDAL storage. Requires a Tokio runtime.

mod config;
pub use config::{LanceFileVersion, LanceOptions};
mod blob;
mod blob_config;
pub use blob_config::{BlobHandleConfig, ExternalBlobMode};
mod blob_writer;
mod committer;
mod compression;
mod error;
mod provider;
mod sink;
mod upload;
mod validation;
mod write_options;

pub use sink::LanceDatasetSink;

mod index;
mod io_metrics;

mod parallel;

mod destination;
pub use open_arrow_sink_storage::{Storage, Url};
