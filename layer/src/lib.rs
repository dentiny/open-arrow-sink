//! Optional sink decorators.
mod ordering;
pub use ordering::{OrderingLayer, OrderingSink};

#[cfg(feature = "rebatch")]
mod rebatch;
#[cfg(feature = "rebatch")]
pub use rebatch::{RebatchLayer, RebatchSink};

mod download_pool;
mod error;
pub use download_pool::DownloadPool;

mod validation;
