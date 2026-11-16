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

pub(crate) fn invalid_compression_level(level: i32) -> Error {
    Error::InvalidConfig(format!(
        "Lance Zstd compression level must be between 0 and {}: {level}",
        open_arrow_sink_core::ZSTD_COMPRESSION_LEVEL_RANGE.end(),
    ))
}

pub(crate) fn unsupported_compression(compression: open_arrow_sink_core::Compression) -> Error {
    Error::Unsupported(format!("Lance compression: {compression:?}"))
}

pub(crate) fn invalid_compression_path(path: &[String]) -> Error {
    Error::Unsupported(format!(
        "nested compression path must identify one field: {path:?}"
    ))
}

pub(crate) fn nested_blob_compression(path: &[String]) -> Error {
    Error::Unsupported(format!(
        "nested compression inside native blob column: {path:?}"
    ))
}

pub(crate) fn blob_structural_encoding(column: &str) -> Error {
    Error::Unsupported(format!("structural encoding on blob column {column}"))
}

pub(crate) fn missing_index_column(column: &str) -> Error {
    Error::InvalidConfig(format!("index column not found: {column}"))
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

pub(crate) fn storage_version_mismatch(
    requested: impl std::fmt::Display,
    existing: impl std::fmt::Display,
) -> Error {
    Error::InvalidConfig(format!(
        "Lance append storage version {requested} does not match existing dataset version {existing}"
    ))
}

pub(crate) fn schema_mismatch() -> Error {
    Error::SchemaMismatch
}
pub(crate) fn poisoned() -> Error {
    Error::Poisoned
}
pub(crate) fn destination_exists() -> Error {
    Error::DestinationExists
}
pub(crate) fn different_dataset_versions() -> Error {
    Error::InvalidInput("Lance writers observed different dataset versions".into())
}
pub(crate) fn invalid_create_fragments() -> Error {
    Error::InvalidInput("expected compatible uncommitted Lance write fragments".into())
}
pub(crate) fn empty_writer_session() -> Error {
    Error::InvalidInput("empty Lance writer session".into())
}
pub(crate) fn parallel_blobs() -> Error {
    Error::Unsupported("parallel sessions do not support blob-marked columns".into())
}
pub(crate) fn blob_column_out_of_bounds() -> Error {
    Error::InvalidInput("blob column out of bounds".into())
}
pub(crate) fn invalid_blob_cell() -> Error {
    Error::InvalidInput("blob placement must target a blob-marked cell".into())
}
pub(crate) fn invalid_blob_placeholder() -> Error {
    Error::InvalidInput("blob placement must replace an empty non-null payload".into())
}
pub(crate) fn foreign_blob() -> Error {
    Error::InvalidInput("foreign blob handle".into())
}
pub(crate) fn blob_owner() -> Error {
    Error::InvalidInput("blob handle belongs to another session".into())
}
pub(crate) fn duplicate_blob_placement() -> Error {
    Error::InvalidInput("duplicate blob placement".into())
}
pub(crate) fn index_exists(name: &str) -> Error {
    Error::Unsupported(format!(
        "index {name} already exists; select Keep or Rebuild"
    ))
}
