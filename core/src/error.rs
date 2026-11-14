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

pub(crate) fn invalid_target_file_size() -> Error {
    Error::Unsupported("target_file_size must be greater than zero".into())
}

pub(crate) fn preserve_input_unsupported() -> Error {
    Error::Unsupported("preserving input row order".into())
}
