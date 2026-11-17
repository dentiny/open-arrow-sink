use open_arrow_sink_core::Error;

use std::io;

pub(crate) fn worker_stopped() -> Error {
    Error::backend(io::Error::other("Lance write worker stopped"))
}

pub(crate) fn index_creation(
    spec: &open_arrow_sink_core::IndexSpec,
    error: impl std::fmt::Display,
) -> lance::Error {
    lance::Error::index(format!(
        "creating {:?} index on '{}': {error}",
        spec.index_type, spec.column
    ))
}

pub(crate) fn native_storage(error: impl std::fmt::Display) -> lance::Error {
    lance::Error::invalid_input(error.to_string())
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

pub(crate) fn unsupported_compression(compression: open_arrow_sink_core::CompressionSpec) -> Error {
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

pub(crate) fn different_dataset_versions() -> Error {
    Error::InvalidInput("Lance writers observed different dataset versions".into())
}
pub(crate) fn invalid_create_fragments() -> Error {
    Error::InvalidInput("expected compatible uncommitted Lance write fragments".into())
}
pub(crate) fn empty_writer_session() -> Error {
    Error::InvalidInput("empty Lance writer session".into())
}

pub(crate) fn invalid_destination_root() -> Error {
    Error::InvalidInput("destination must not be the storage root".into())
}

pub(crate) fn blob_input(column: &str, row: usize, source: Error) -> Error {
    source.context(format_args!("blob column {column} row {row}"))
}
