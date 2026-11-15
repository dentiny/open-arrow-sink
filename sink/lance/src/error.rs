use open_arrow_sink_core::{Error, ZSTD_COMPRESSION_LEVEL_RANGE};

use std::io;

use arrow_schema::ArrowError;

pub(crate) fn backend(error: impl std::error::Error + Send + Sync + 'static) -> Error {
    Error::Backend(Box::new(error))
}

pub(crate) fn invalid_compression_level(level: i32) -> Error {
    Error::Unsupported(format!(
        "Lance Zstd compression level must be between 0 and {}: {level}",
        ZSTD_COMPRESSION_LEVEL_RANGE.end(),
    ))
}

pub(crate) fn aborted() -> ArrowError {
    ArrowError::ExternalError(Box::new(io::Error::new(
        io::ErrorKind::Interrupted,
        "Lance writer aborted or dropped",
    )))
}

pub(crate) fn failed() -> Error {
    backend(io::Error::other("writer failed; abort or drop it"))
}

pub(crate) fn worker_stopped() -> Error {
    backend(io::Error::other("Lance write worker stopped"))
}

pub(crate) fn invalid_path() -> Error {
    backend(io::Error::new(
        io::ErrorKind::InvalidInput,
        "Lance requires a UTF-8 path",
    ))
}

pub(crate) fn missing_index_column(column: &str) -> Error {
    Error::Unsupported(format!("index column not found: {column}"))
}

pub(crate) fn unsupported_index(
    index: &open_arrow_sink_core::IndexSpec,
    data_type: &arrow_schema::DataType,
) -> Error {
    Error::Unsupported(format!(
        "cannot create {:?} index on column {} with type {}",
        index.index_type, index.column, data_type
    ))
}

pub(crate) fn index_creation(
    spec: &open_arrow_sink_core::IndexSpec,
    error: impl std::fmt::Display,
) -> open_arrow_sink_core::IndexCommitError {
    open_arrow_sink_core::IndexCommitError {
        message: format!(
            "creating {:?} index on '{}': {error}",
            spec.index_type, spec.column
        ),
    }
}

pub(crate) fn index_task(error: impl std::fmt::Display) -> open_arrow_sink_core::IndexCommitError {
    open_arrow_sink_core::IndexCommitError {
        message: format!("index construction task failed: {error}"),
    }
}

pub(crate) fn index_refresh(
    error: impl std::fmt::Display,
) -> open_arrow_sink_core::IndexCommitError {
    open_arrow_sink_core::IndexCommitError {
        message: format!("refreshing dataset after index construction: {error}"),
    }
}
