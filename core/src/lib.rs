//! Contracts shared by sinks and layers. Backend implementations own I/O and commits.
mod capability;
mod compression;
mod error;
mod guard;
pub use guard::{GuardedDataWriter, GuardedWriter};
mod index;
mod io_metrics;
pub use io_metrics::{IoMetrics, IoObservers, IoSnapshot};
mod layer;
mod poison;
mod request;
pub use poison::Poison;
mod session;
mod sink;
pub mod validation;
mod write_all;
mod writer;
pub use write_all::WriteSummary;

pub use arrow_array::RecordBatch;
pub use arrow_schema::SchemaRef;
pub use async_trait::async_trait;
pub use capability::{PartitionType, SinkCapability};
pub use compression::{
    CompressionCapability, CompressionCodec, CompressionOptions, CompressionSpec,
    StructuralEncoding, ZSTD_COMPRESSION_LEVEL_RANGE,
};
pub use error::{Error, IndexCommitError, Result, WriteAllError};
pub use index::{ExistingIndexPolicy, IndexSpec, IndexType};
pub use layer::{Layer, SinkExt};
pub use request::{Ordering, PartitionOptions, UploadConfig, WriteMode, WriteOptions};
pub use sink::{ParallelSink, Sink};
pub use writer::Writer;

mod blob;
pub use blob::{BlobChunk, BlobHandleConfig, ExternalBlobMode};

/// Field metadata marking blob inputs: binary payloads or explicitly selected URL strings.
pub const BLOB_METADATA_KEY: &str = "open-arrow-sink:blob";

/// Metadata value enabling native blob handling for a field.
pub const BLOB_METADATA_VALUE: &str = "true";

/// Whether a field explicitly opts into native blob handling.
/// Missing metadata and values other than the exact string `"true"` are disabled.
pub fn is_blob_field(field: &arrow_schema::Field) -> bool {
    field.metadata().get(BLOB_METADATA_KEY).map(String::as_str) == Some(BLOB_METADATA_VALUE)
}

pub use session::{DataWriter, SessionCommitter, WriteSession};

mod download_metrics;
pub use download_metrics::{
    DownloadFailurePolicy, DownloadMetrics, DownloadRecovery, DownloadSnapshot,
};
