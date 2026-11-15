use std::io;

use open_arrow_sink_core::Error;

pub(crate) fn backend(error: impl std::error::Error + Send + Sync + 'static) -> Error {
    Error::Backend(Box::new(error))
}

pub(crate) fn failure(message: impl Into<String>) -> Error {
    backend(io::Error::other(message.into()))
}

pub(crate) fn unsupported() -> Error {
    Error::Unsupported("writer failed or was cancelled; abort or drop it".into())
}
