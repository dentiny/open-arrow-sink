//! Composable sink decorators.
mod compression;
mod download_blob;
mod error;
mod metrics;
mod ordering;
mod schema_evolution;
mod unordered;
mod validation;

pub use compression::{CompressionLayer, CompressionSink};
pub use download_blob::{DownloadBlobLayer, DownloadBlobSink};
pub use metrics::{MetricsLayer, MetricsSink, MetricsSnapshot, WriteMetrics};
pub use ordering::{OrderingLayer, OrderingSink};
pub use schema_evolution::{FieldRemap, SchemaEvolutionLayer, SchemaEvolutionSink};
pub use unordered::{UnorderedWriteLayer, UnorderedWriteSink};

/// Convenience re-export of the compression-level range owned by `open-arrow-sink-core`.
pub use open_arrow_sink_core::ZSTD_COMPRESSION_LEVEL_RANGE;
/// Convenience re-export of the download pool owned by `open-arrow-sink-storage`.
pub use open_arrow_sink_storage::DownloadPool;
