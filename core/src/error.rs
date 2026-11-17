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
    /// A destination or requested index already exists.
    #[error("already exists: {0}")]
    AlreadyExists(String),
    #[error("invalid configuration: {0}")]
    InvalidConfig(String),
    /// A malformed URL, path, schema reference, or blob placement supplied by the caller.
    #[error("invalid input: {0}")]
    InvalidInput(String),
    #[error("not found: {0}")]
    NotFound(String),
    /// Data is visible; retry index construction rather than replaying the write.
    #[error(transparent)]
    IndexCommit(#[from] IndexCommitError),
    #[error("backend error: {0}")]
    Backend(#[source] Box<dyn std::error::Error + Send + Sync>),
}

impl Error {
    /// Add context while preserving the error category and backend source chain.
    ///
    /// String-bearing categories receive a message prefix. `Backend` wraps the
    /// original error, retaining native errors for source-chain inspection.
    /// `Poisoned`, `SchemaMismatch`, and `IndexCommit` keep their original values
    /// without adding context, so lifecycle and publication signals stay intact.
    pub fn context(self, context: impl std::fmt::Display) -> Self {
        let prefix = |message| format!("{context}: {message}");
        match self {
            Self::Unsupported(message) => Self::Unsupported(prefix(message)),
            Self::AlreadyExists(message) => Self::AlreadyExists(prefix(message)),
            Self::InvalidConfig(message) => Self::InvalidConfig(prefix(message)),
            Self::InvalidInput(message) => Self::InvalidInput(prefix(message)),
            Self::NotFound(message) => Self::NotFound(prefix(message)),
            source @ Self::Backend(_) => Self::backend(ContextError {
                context: context.to_string(),
                source,
            }),
            error => error,
        }
    }

    /// Preserve a native error and its source chain at a backend boundary.
    pub fn backend(error: impl std::error::Error + Send + Sync + 'static) -> Self {
        Self::Backend(Box::new(error))
    }
}

/// Index construction failed after data was committed.
/// Returned as `Error::IndexCommit`; data is already committed.
/// Data is already visible: retry index construction rather than the data write.
#[derive(Debug, thiserror::Error)]
#[error("data committed; index construction failed: {message}")]
#[non_exhaustive]
pub struct IndexCommitError {
    pub message: String,
}

impl IndexCommitError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

#[derive(Debug, thiserror::Error)]
#[error("{context}: {source}")]
struct ContextError {
    context: String,
    #[source]
    source: Error,
}
