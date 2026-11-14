//! Create-only local Lance datasets. Requires a Tokio runtime.

mod error;
mod sink;
mod validation;
mod writer;

pub use error::IndexCommitError;
pub use sink::LanceDatasetSink;

mod index;
pub use index::{
    IndexBuildReport, IndexBuildResult, IndexBuildState, IndexedLanceSink, LanceIndexKind,
    LanceIndexLayer, LanceIndexSpec,
};
