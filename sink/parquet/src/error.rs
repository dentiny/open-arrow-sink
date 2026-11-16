use std::io;

use open_arrow_sink_core::Error;

pub(crate) fn parquet_backend(error: Error) -> parquet::errors::ParquetError {
    parquet::errors::ParquetError::External(Box::new(error))
}

pub(crate) fn not_directory() -> Error {
    Error::backend(io::Error::other("destination must be a directory"))
}
pub(crate) fn missing_directory() -> Error {
    Error::backend(io::Error::new(
        io::ErrorKind::NotFound,
        "destination directory does not exist",
    ))
}
pub(crate) fn overwrite_unsupported() -> Error {
    Error::Unsupported("Parquet directory sink does not support Overwrite".into())
}
