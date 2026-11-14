//! Create-only local Lance datasets. Requires a Tokio runtime.

mod error;
mod sink;
mod writer;

pub use sink::LanceDatasetSink;
