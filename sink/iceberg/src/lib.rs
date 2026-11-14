//! Append batches to an existing Iceberg table.

mod error;
mod partition;
mod sink;
mod validation;
mod writer;

pub use iceberg;
pub use sink::IcebergTableSink;
