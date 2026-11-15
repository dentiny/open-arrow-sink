//! Create-only local directory sink with optional target-size Parquet file rolling.

mod compression;
mod error;
mod sink;
mod writer;

pub use sink::ParquetDirectorySink;
