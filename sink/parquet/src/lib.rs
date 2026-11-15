//! Create-only URL-based directory sink with optional target-size Parquet file rolling.

mod committer;
mod compression;
mod destination;
mod error;
mod sink;
mod writer;

pub use sink::ParquetDirectorySink;
pub use url::Url;

#[cfg(test)]
mod tests;
