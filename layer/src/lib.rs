//! Optional sink decorators.
mod ordering;
pub use ordering::{OrderingLayer, OrderingSink};

mod unordered;
pub use unordered::{UnorderedWriteLayer, UnorderedWriteSink};

#[cfg(feature = "rebatch")]
mod rebatch;
#[cfg(feature = "rebatch")]
pub use rebatch::{RebatchLayer, RebatchSink};
