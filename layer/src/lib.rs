//! Composable sink decorators.
mod error;
mod ordering;
pub use ordering::{OrderingLayer, OrderingSink};

mod unordered;
pub use unordered::{UnorderedWriteLayer, UnorderedWriteSink};
