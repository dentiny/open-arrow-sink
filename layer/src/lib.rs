//! Optional sink decorators.
mod ordering;
pub use ordering::{OrderingLayer, OrderingSink};

#[cfg(feature = "rebatch")]
mod rebatch;
#[cfg(feature = "rebatch")]
pub use rebatch::{RebatchLayer, RebatchSink};

mod compression;
pub use compression::{CompressionLayer, CompressionSink};
