pub type Result<T> = std::result::Result<T, Error>;

/// A write error. Callers matching individual variants must retain a fallback
/// arm so future error categories can be added without breaking their code.
///
/// ```compile_fail,E0004
/// use open_arrow_sink_core::Error;
/// fn inspect(error: Error) {
///     match error {
///         Error::Unsupported(_) => (),
///         Error::SchemaMismatch => (),
///         Error::Backend(_) => (),
///     }
/// }
/// ```
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    #[error("writer failed, was cancelled, or is already finished; abort or drop it")]
    Poisoned,
    #[error("unsupported operation: {0}")]
    Unsupported(String),
    #[error("batch schema does not match the write session schema")]
    SchemaMismatch,
    #[error("backend error: {0}")]
    Backend(#[source] Box<dyn std::error::Error + Send + Sync>),
}

pub(crate) fn invalid_target_file_size() -> Error {
    Error::Unsupported("target_file_size must be greater than zero".into())
}

pub(crate) fn preserve_input_unsupported() -> Error {
    Error::Unsupported("preserving input row order".into())
}

/// Index construction failed after data was committed.
/// Returned inside `Error::Backend`; downcast to inspect the committed summary.
/// Data is already visible: retry index construction rather than the data write.
#[derive(Debug, thiserror::Error)]
#[error("data committed; index construction failed: {message}")]
#[non_exhaustive]
pub struct IndexCommitError {
    pub message: String,
    /// The data write succeeded before index construction failed.
    pub summary: crate::WriteSummary,
}

impl IndexCommitError {
    pub fn new(message: impl Into<String>, summary: crate::WriteSummary) -> Self {
        Self {
            message: message.into(),
            summary,
        }
    }
}

pub(crate) fn unsupported_partition(partition: crate::PartitionType) -> Error {
    Error::Unsupported(format!("partitioned writes with {partition:?}"))
}

pub(crate) fn unsupported_index(index: &crate::IndexSpec) -> Error {
    Error::Unsupported(format!(
        "{:?} index construction on column {}",
        index.index_type, index.column
    ))
}

pub(crate) fn duplicate_index(index: &crate::IndexSpec) -> Error {
    Error::Unsupported(format!(
        "duplicate {:?} index requested for column {}",
        index.index_type, index.column
    ))
}

pub(crate) fn invalid_index_column(column: &str) -> Error {
    Error::Unsupported(format!(
        "index column must identify one top-level field: {column}"
    ))
}

pub(crate) fn unsupported_blob_streams() -> Error {
    Error::Unsupported("sink does not support streaming blobs".into())
}
