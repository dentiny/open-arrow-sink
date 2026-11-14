//! Contracts shared by sinks and layers. Backend implementations own I/O and commits.
mod capability;
mod error;
mod index;
mod layer;
mod request;
mod result;
mod sink;
pub mod validation;
mod writer;

pub use arrow_array::RecordBatch;
pub use arrow_schema::SchemaRef;
pub use async_trait::async_trait;
pub use capability::{PartitionType, SinkCapability};
pub use error::{Error, IndexCommitError, Result};
pub use index::{IndexSpec, IndexType};
pub use layer::{Layer, SinkExt};
pub use request::{Ordering, PartitionOptions, WriteOptions};
pub use result::WriteSummary;
pub use sink::Sink;
pub use writer::Writer;

/// Field metadata marking downloaded `LargeBinary` values as blob payloads.
/// Backends may map these fields to native blob storage.
pub const BLOB_METADATA_KEY: &str = "open-arrow-sink:blob";
