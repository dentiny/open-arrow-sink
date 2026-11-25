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

pub use arrow_array::RecordBatch;
pub use arrow_schema::SchemaRef;
pub use async_trait::async_trait;
pub use capability::{PartitionType, SinkCapability};
pub use compression::{
    CompressionCapability, CompressionCodec, CompressionOptions, CompressionSpec,
    StructuralEncoding, ZSTD_COMPRESSION_LEVEL_RANGE,
};
pub use error::{Error, IndexCommitError, Result};
pub use index::{ExistingIndexPolicy, IndexSpec, IndexType};
pub use layer::{Layer, SinkExt};
pub use request::{Ordering, PartitionOptions, UploadConfig, WriteMode, WriteOptions};
pub use sink::{ParallelSink, Sink};
pub use writer::{CommitSummary, Writer};

mod blob;
pub use blob::{BlobChunk, BlobHandleConfig, ExternalBlobMode};

/// Field metadata marking blob inputs: binary payloads or explicitly selected URL strings.
pub const BLOB_METADATA_KEY: &str = "open-arrow-sink:blob";

/// Existing binary payload bytes; no source download is needed.
pub const BLOB_BYTES_VALUE: &str = "bytes";

/// A source URI consumed according to the configured ingest/reference policy.
pub const BLOB_URI_VALUE: &str = "uri";

/// Legacy metadata value enabling type-inferred blob handling.
pub const BLOB_METADATA_VALUE: &str = "true";

/// Whether a field explicitly opts into native blob handling.
/// Recognizes explicit `bytes`/`uri` markers and legacy `true`. Backends validate
/// the declared kind against the field type. Missing/other values stay disabled.
pub fn is_blob_field(field: &arrow_schema::Field) -> bool {
    matches!(
        field.metadata().get(BLOB_METADATA_KEY).map(String::as_str),
        Some(BLOB_BYTES_VALUE | BLOB_URI_VALUE | BLOB_METADATA_VALUE)
    )
}

pub use session::{DataWriter, SessionCommitter, WriteSession};

mod download_metrics;
pub use download_metrics::{
    DownloadFailurePolicy, DownloadMetrics, DownloadRecovery, DownloadSnapshot,
};
