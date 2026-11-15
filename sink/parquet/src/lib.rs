//! Local local directory sink with optional target-size Parquet file rolling.

mod committer;
mod compression;
mod destination;
mod error;
mod sink;
mod validation;
mod writer;

pub use sink::ParquetDirectorySink;
