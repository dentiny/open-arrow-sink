//! Append batches to an existing Iceberg table.

mod committer;
mod compression;
mod error;
mod partition;
mod sink;
mod validation;
mod writer;

pub use iceberg;
pub use sink::IcebergTableSink;

mod storage;
pub use open_arrow_sink_storage::Storage;
pub use storage::OpenDalStorageFactory;
