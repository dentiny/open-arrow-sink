use open_arrow_sink_core::Error;

use std::io;

pub(crate) fn worker_stopped() -> Error {
    Error::backend(io::Error::other("Lance write worker stopped"))
}

pub(crate) fn index_creation(
    spec: &open_arrow_sink_core::IndexSpec,
    error: impl std::fmt::Display,
    summary: open_arrow_sink_core::WriteSummary,
) -> open_arrow_sink_core::IndexCommitError {
    open_arrow_sink_core::IndexCommitError::new(
        format!(
            "creating {:?} index on '{}': {error}",
            spec.index_type, spec.column
        ),
        summary,
    )
}

pub(crate) fn index_task(
    error: impl std::fmt::Display,
    summary: open_arrow_sink_core::WriteSummary,
) -> open_arrow_sink_core::IndexCommitError {
    open_arrow_sink_core::IndexCommitError::new(
        format!("index construction task failed: {error}"),
        summary,
    )
}

pub(crate) fn index_refresh(
    error: impl std::fmt::Display,
    summary: open_arrow_sink_core::WriteSummary,
) -> open_arrow_sink_core::IndexCommitError {
    open_arrow_sink_core::IndexCommitError::new(
        format!("refreshing dataset after index construction: {error}"),
        summary,
    )
}

pub(crate) fn native_storage(error: impl std::fmt::Display) -> lance::Error {
    lance::Error::invalid_input(error.to_string())
}

pub(crate) fn invalid_blob_column(column: &str) -> Error {
    Error::InvalidInput(format!("blob column must be LargeBinary: {column}"))
}
pub(crate) fn unfinished_blobs() -> Error {
    Error::backend(io::Error::other(
        "writer failed or has unfinished blobs; session aborted",
    ))
}

pub(crate) fn unsupported_storage_version(version: impl std::fmt::Display) -> Error {
    Error::Unsupported(format!(
        "Lance storage version must be 2.2 or 2.3: {version}"
    ))
}
