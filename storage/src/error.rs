use std::io;

use open_arrow_sink_core::{Error, Result};
use opendal::ErrorKind;

fn invalid(message: &str) -> Error {
    Error::Backend(Box::new(io::Error::new(
        io::ErrorKind::InvalidInput,
        message.to_owned(),
    )))
}

pub(crate) fn backend(error: opendal::Error) -> Error {
    Error::Backend(Box::new(error))
}

pub(crate) fn invalid_file_url() -> Error {
    invalid("invalid file URL")
}
pub(crate) fn endpoint_mismatch() -> Error {
    invalid("URL does not belong to this storage endpoint")
}
pub(crate) fn outside_root() -> Error {
    invalid("URL is outside the storage root")
}
pub(crate) fn url_without_paths() -> Error {
    invalid("URL cannot contain paths")
}
pub(crate) fn invalid_read_range() -> Error {
    invalid("invalid read range")
}
pub(crate) fn empty_path() -> Error {
    invalid("operation requires a nonempty path")
}
pub(crate) fn invalid_url() -> Error {
    invalid("storage URLs must be hierarchical and contain no query, fragment, or credentials")
}
pub(crate) fn invalid_utf8_path() -> Error {
    invalid("URL path must be UTF-8")
}
pub(crate) fn invalid_path() -> Error {
    invalid("invalid storage path")
}
pub(crate) fn invalid_retry() -> Error {
    invalid("invalid retry delay or factor")
}
pub(crate) fn invalid_timeout() -> Error {
    invalid("timeouts must be positive")
}
pub(crate) fn writer_inactive() -> Error {
    invalid("writer is no longer active")
}
pub(crate) fn writer_closed() -> Error {
    invalid("writer is closed")
}

pub(crate) fn delete_result(result: opendal::Result<()>) -> Result<()> {
    match result {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(()),
        Err(error) => Err(backend(error)),
    }
}

pub(crate) fn abort_result(result: opendal::Result<()>, filesystem: bool) -> Result<()> {
    match result {
        Ok(()) => Ok(()),
        // Non-staged filesystem writes are already visible. Dropping the
        // handle stops this stream; the owning sink deletes its files.
        Err(error) if filesystem && error.kind() == ErrorKind::Unsupported => Ok(()),
        Err(error) => Err(backend(error)),
    }
}
