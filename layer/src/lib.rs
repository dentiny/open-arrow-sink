//! Composable sink decorators.
mod error;
mod options;
pub use options::{OptionsLayer, OptionsSink};
mod ordering;
mod validation;
pub use ordering::{OrderingLayer, OrderingSink};

mod unordered;
pub use unordered::{UnorderedWriteLayer, UnorderedWriteSink};

mod compression;
pub use compression::{CompressionLayer, CompressionSink};
pub use open_arrow_sink_core::ZSTD_COMPRESSION_LEVEL_RANGE;

mod metrics;
pub use metrics::{MetricsLayer, MetricsSink, MetricsSnapshot, WriteMetrics};

mod schema_evolution;
pub use schema_evolution::{FieldRemap, SchemaEvolutionLayer, SchemaEvolutionSink};
