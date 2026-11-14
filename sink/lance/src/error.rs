use std::io;

use arrow_schema::ArrowError;
use open_arrow_sink_core::Error;

pub(crate) fn backend(error: impl std::error::Error + Send + Sync + 'static) -> Error {
    Error::Backend(Box::new(error))
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
