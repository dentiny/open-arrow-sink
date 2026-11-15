//! Composable sink decorators.
mod error;
mod ordering;
mod validation;
pub use ordering::{OrderingLayer, OrderingSink};

pub use open_arrow_sink_storage::DownloadPool;

mod unordered;
pub use unordered::{UnorderedWriteLayer, UnorderedWriteSink};

mod compression;
pub use compression::{CompressionLayer, CompressionSink};
pub use open_arrow_sink_core::ZSTD_COMPRESSION_LEVEL_RANGE;

#[cfg(feature = "download-blob")]
mod download_blob;
#[cfg(feature = "download-blob")]
pub use download_blob::{DownloadBlobLayer, DownloadBlobSink};
