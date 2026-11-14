//! Create-only local directory sink: one uncompressed Parquet file per session.

mod error;
mod sink;
mod writer;

pub use sink::ParquetDirectorySink;
