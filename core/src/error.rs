use std::fmt;

use crate::IndexBuildReport;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("unsupported operation: {0}")]
    Unsupported(String),
    #[error("batch schema does not match the write session schema")]
    SchemaMismatch,
    #[error("backend error: {0}")]
    Backend(#[source] Box<dyn std::error::Error + Send + Sync>),
}

/// Returned inside `open_arrow_sink_core::Error::Backend`; downcast to inspect.
#[derive(Debug)]
pub struct IndexCommitError {
    pub report: IndexBuildReport,
    pub message: String,
}

impl fmt::Display for IndexCommitError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "data committed at {:?}; indexing {} failed: {}",
            self.report.data_version, self.report.destination, self.message
        )
    }
}
impl std::error::Error for IndexCommitError {}
