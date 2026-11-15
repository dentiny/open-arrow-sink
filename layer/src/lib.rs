//! Composable sink decorators.
mod error;
mod ordering;
pub use ordering::{OrderingLayer, OrderingSink};

mod unordered;
pub use unordered::{UnorderedWriteLayer, UnorderedWriteSink};

mod compression;
pub use compression::{CompressionLayer, CompressionSink};
pub use open_arrow_sink_core::ZSTD_COMPRESSION_LEVEL_RANGE;
