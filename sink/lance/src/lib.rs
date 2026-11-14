//! Create-only local Lance datasets. Requires a Tokio runtime.

mod blob;
mod error;
mod sink;
mod validation;
mod writer;

pub use sink::LanceDatasetSink;

mod index;
