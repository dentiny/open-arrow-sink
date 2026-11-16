//! Contracts shared by sinks and layers. Backend implementations own I/O and commits.
mod blob;
mod capability;
mod compression;
mod error;
mod index;
mod io_metrics;
mod layer;
mod request;
mod result;
mod session;
mod sink;
pub mod validation;
mod writer;

pub use arrow_array::RecordBatch;
pub use arrow_schema::SchemaRef;
pub use async_trait::async_trait;
pub use blob::{BlobChunk, BlobHandle, BlobPlacement, BlobWriter, is_blob_marked};
pub use capability::{PartitionType, SinkCapability};
pub use compression::{
    Compression, CompressionCapability, CompressionCodec, CompressionOptions, StructuralEncoding,
    ZSTD_COMPRESSION_LEVEL_RANGE,
};
pub use error::{Error, IndexCommitError, Result};
pub use index::{ExistingIndexPolicy, IndexSpec, IndexType};
pub use io_metrics::{IoMetrics, IoMetricsRegistration, IoMetricsSource, IoSnapshot};
pub use layer::{Layer, SinkExt};
pub use request::{Ordering, PartitionOptions, WriteMode, WriteOptions};
pub use result::WriteSummary;
pub use session::{DataWriter, SessionCommitter, WriteSession};
pub use sink::{ParallelSink, Sink};
pub use writer::Writer;

/// Field metadata marking binary values that backends may map to native blobs.
pub const BLOB_METADATA_KEY: &str = "open-arrow-sink:blob";
