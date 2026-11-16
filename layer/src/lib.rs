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

mod download_blob;
pub use download_blob::{
    DownloadBlobLayer, DownloadBlobSink, DownloadFailurePolicy, DownloadMetrics, DownloadSnapshot,
};

mod metrics;
pub use metrics::{MetricsLayer, MetricsSink, MetricsSnapshot, WriteMetrics};

mod schema_evolution;
pub use schema_evolution::{FieldRemap, SchemaEvolutionLayer, SchemaEvolutionSink};
