use std::io;

use open_arrow_sink_core::Error;

pub(crate) fn backend(error: impl std::error::Error + Send + Sync + 'static) -> Error {
    Error::Backend(Box::new(error))
}

pub(crate) fn closed() -> Error {
    backend(io::Error::other("writer is no longer active"))
}

pub(crate) fn empty_destination() -> Error {
    backend(io::Error::other("destination must have a nonempty path"))
}

pub(crate) fn destination_exists() -> Error {
    backend(io::Error::new(
        io::ErrorKind::AlreadyExists,
        "destination exists",
    ))
}

pub(crate) fn parquet_backend(error: Error) -> parquet::errors::ParquetError {
    parquet::errors::ParquetError::External(Box::new(error))
}

pub(crate) fn not_directory() -> Error {
    backend(io::Error::other(
        "destination must be a directory, not a file or symlink",
    ))
}
pub(crate) fn missing_directory() -> Error {
    backend(io::Error::new(
        io::ErrorKind::NotFound,
        "destination directory does not exist",
    ))
}
pub(crate) fn overwrite_unsupported() -> Error {
    Error::Unsupported(
        "Parquet staged overwrite requires a filesystem backend with directory rename".into(),
    )
}
pub(crate) fn schema_mismatch() -> Error {
    Error::SchemaMismatch
}
