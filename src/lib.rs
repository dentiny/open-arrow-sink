//! Independent Arrow write contracts and composable layers.
//! No dependency on open-arrow-access is required.
pub use open_arrow_sink_core::*;
pub use open_arrow_sink_layer as layer;

#[cfg(feature = "sink-iceberg")]
pub use open_arrow_sink_iceberg as iceberg;
#[cfg(feature = "sink-parquet")]
pub use open_arrow_sink_parquet as parquet;
