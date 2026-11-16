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

/// What is known about data publication when an operation returns an error.
/// This describes publication, not whether a particular retry mode will succeed:
/// reservations and orphan files can remain even when nothing was published.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum PublicationOutcome {
    /// The backend guarantees this session did not publish data/table metadata.
    NotPublished,
    /// Publication may have succeeded. Inspect the destination before retrying.
    Unknown,
    /// Data publication succeeded; a subsequent operation failed.
    Published,
}

/// Typed publication context stored in `Error::Backend`, retaining the original
/// error in the standard source chain.
#[derive(Debug, thiserror::Error)]
#[error("publication outcome {outcome:?}: {source}")]
pub struct PublicationError {
    pub outcome: PublicationOutcome,
    #[source]
    source: Error,
}

impl Error {
    /// Inspect typed publication context without parsing backend error messages.
    /// Unclassified errors (including third-party sinks) conservatively return
    /// `Unknown`. `IndexCommitError` always means data was already published.
    pub fn publication_outcome(&self) -> PublicationOutcome {
        let mut current: &(dyn std::error::Error + 'static) = self;
        loop {
            if let Some(error) = current.downcast_ref::<PublicationError>() {
                return error.outcome;
            }
            if current.is::<IndexCommitError>() {
                return PublicationOutcome::Published;
            }
            match current.source() {
                Some(source) => current = source,
                None => return PublicationOutcome::Unknown,
            }
        }
    }

    /// Backend/layer adapters attach a proven publication boundary. A known
    /// inner outcome is preserved instead of being downgraded by an outer layer.
    pub fn with_publication_outcome(self, outcome: PublicationOutcome) -> Self {
        let known = self.publication_outcome();
        let outcome = if known == PublicationOutcome::Unknown {
            outcome
        } else {
            known
        };
        Self::Backend(Box::new(PublicationError {
            outcome,
            source: self,
        }))
    }
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
