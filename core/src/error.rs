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

/// Index construction failed after data was committed.
/// Returned inside `Error::Backend`; downcast to inspect the error context.
#[derive(Debug, thiserror::Error)]
#[error("data committed; index construction failed: {message}")]
pub struct IndexCommitError {
    pub message: String,
}
