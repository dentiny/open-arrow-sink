//! Create-only local Lance datasets. Requires a Tokio runtime.

mod error;
mod sink;
mod writer;

pub use sink::LanceDatasetSink;

mod index;
pub use index::{
    IndexBuildReport, IndexBuildResult, IndexBuildState, IndexCommitError, IndexedLanceSink,
    LanceIndexKind, LanceIndexLayer, LanceIndexSpec, retry_indexes,
};
