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

pub(crate) fn worker_stopped() -> Error {
    backend(io::Error::other("Lance write worker stopped"))
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

pub(crate) fn destination_exists() -> Error {
    backend(io::Error::new(
        io::ErrorKind::AlreadyExists,
        "destination exists",
    ))
}

pub(crate) fn native_storage(error: impl std::fmt::Display) -> lance::Error {
    lance::Error::invalid_input(error.to_string())
}

pub(crate) fn invalid_create_fragments() -> Error {
    Error::Unsupported("expected compatible uncommitted Lance write fragments".into())
}

pub(crate) fn empty_writer_session() -> Error {
    Error::Unsupported("empty Lance writer session".into())
}

pub(crate) fn different_dataset_versions() -> Error {
    Error::Unsupported("Lance writers observed different dataset versions".into())
}

pub(crate) fn invalid_blob(message: &'static str) -> Error {
    Error::Unsupported(message.into())
}
pub(crate) fn invalid_blob_column(column: &str) -> Error {
    Error::Unsupported(format!("blob column must be LargeBinary: {column}"))
}
pub(crate) fn unfinished_blobs() -> Error {
    backend(io::Error::other(
        "writer failed or has unfinished blobs; session aborted",
    ))
}

pub(crate) fn schema_mismatch() -> Error {
    Error::SchemaMismatch
}

pub(crate) fn blob_column_out_of_bounds() -> Error {
    invalid_blob("blob column out of bounds")
}

pub(crate) fn blob_cell() -> Error {
    invalid_blob("blob placement must target a blob-marked cell")
}

pub(crate) fn blob_placeholder() -> Error {
    invalid_blob("blob placement must replace an empty non-null payload")
}

pub(crate) fn foreign_blob() -> Error {
    invalid_blob("foreign blob handle")
}

pub(crate) fn blob_owner() -> Error {
    invalid_blob("blob handle belongs to another session")
}

pub(crate) fn duplicate_blob_placement() -> Error {
    invalid_blob("duplicate blob placement")
}

pub(crate) fn unsupported_storage_version(version: impl std::fmt::Display) -> Error {
    Error::Unsupported(format!(
        "Lance storage version must be 2.2 or 2.3: {version}"
    ))
}

pub(crate) fn storage_version_mismatch(
    requested: impl std::fmt::Display,
    existing: impl std::fmt::Display,
) -> Error {
    Error::Unsupported(format!(
        "Lance append storage version {requested} does not match existing dataset version {existing}"
    ))
}

pub(crate) fn mixed_blob_inputs() -> Error {
    Error::Unsupported(
        "cannot mix logical blob writes and prepared streaming handles in one session".into(),
    )
}
