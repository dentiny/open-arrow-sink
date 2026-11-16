pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, thiserror::Error)]
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
/// Returned inside `Error::Backend`; downcast to inspect the error context.
#[derive(Debug, thiserror::Error)]
#[error("data committed; index construction failed: {message}")]
pub struct IndexCommitError {
    pub message: String,
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

pub(crate) fn duplicate_index(column: &str) -> Error {
    Error::Unsupported(format!("multiple indexes requested for column {column}"))
}

pub(crate) fn invalid_index_column(column: &str) -> Error {
    Error::Unsupported(format!(
        "index column must identify one top-level field: {column}"
    ))
}

pub(crate) fn unsupported_blob_streams() -> Error {
    Error::Unsupported("sink does not support streaming blobs".into())
}
