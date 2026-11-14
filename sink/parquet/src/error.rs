use std::io;

use open_arrow_sink_core::Error;

pub(crate) fn backend(error: impl std::error::Error + Send + Sync + 'static) -> Error {
    Error::Backend(Box::new(error))
}

pub(crate) fn closed() -> Error {
    backend(io::Error::other("writer is no longer active"))
}
