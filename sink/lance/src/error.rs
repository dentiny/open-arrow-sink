use std::fmt;

use open_arrow_sink_core::Error;

use crate::index::IndexBuildReport;

pub(crate) fn backend(error: impl std::error::Error + Send + Sync + 'static) -> Error {
    Error::Backend(Box::new(error))
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
            self.report.data_version,
            self.report.path.display(),
            self.message
        )
    }
}
impl std::error::Error for IndexCommitError {}
