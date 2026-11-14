//! Append batches to an existing, unpartitioned Iceberg table.

mod error;
mod sink;
mod writer;

pub use iceberg;
pub use sink::IcebergTableSink;
