//! Create-only URL directory sink with optional target-size Parquet file rolling.

mod committer;
mod compression;
mod destination;
mod error;
mod sink;
mod writer;

pub use open_arrow_sink_storage::{Storage, Url};
pub use sink::ParquetDirectorySink;
