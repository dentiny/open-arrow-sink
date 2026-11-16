use std::io;

use open_arrow_sink_core::{Error, Result};
use opendal::ErrorKind;

fn invalid(message: &str) -> Error {
    Error::Backend(Box::new(io::Error::new(
        io::ErrorKind::InvalidInput,
        message.to_owned(),
    )))
}

/// Preserve the operation, object location, and original OpenDAL error.
pub(crate) fn map_error(operation: &'static str, path: &str, source: opendal::Error) -> Error {
    Error::Backend(Box::new(BackendError {
        operation,
        path: path.to_owned(),
        source,
    }))
}

#[derive(Debug)]
struct BackendError {
    operation: &'static str,
    path: String,
    source: opendal::Error,
}
impl std::fmt::Display for BackendError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "{} object {:?}: {}",
            self.operation, self.path, self.source
        )
    }
}
impl std::error::Error for BackendError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.source)
    }
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
pub(crate) fn invalid_retry_factor() -> Error {
    invalid_config("retry factor must be finite and at least 1")
}

pub(crate) fn invalid_retry_delays() -> Error {
    invalid_config("retry min_delay must not exceed max_delay")
}

pub(crate) fn invalid_timeouts() -> Error {
    invalid_config("storage timeouts must be positive")
}

pub(crate) fn invalid_http_concurrency() -> Error {
    invalid_config("HTTP concurrency must be positive and not exceed Semaphore::MAX_PERMITS")
}

fn invalid_config(message: &'static str) -> Error {
    invalid(message)
}

pub(crate) fn writer_closed() -> Error {
    invalid("writer is closed")
}

pub(crate) fn delete_result(path: &str, result: opendal::Result<()>) -> Result<()> {
    match result {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(()),
        Err(error) => Err(map_error("delete", path, error)),
    }
}

pub(crate) fn abort_result(
    path: &str,
    result: opendal::Result<()>,
    filesystem: bool,
) -> Result<()> {
    match result {
        Ok(()) => Ok(()),
        // Non-staged filesystem writes are already visible. Dropping the
        // handle stops this stream; the owning sink deletes its files.
        Err(error) if filesystem && error.kind() == ErrorKind::Unsupported => Ok(()),
        Err(error) => Err(map_error("abort", path, error)),
    }
}

#[cfg(feature = "http")]
pub(crate) fn backend(error: impl std::error::Error + Send + Sync + 'static) -> Error {
    Error::Backend(Box::new(error))
}
#[cfg(feature = "http")]
pub(crate) fn download_limit() -> Error {
    invalid("download pool limits exceed semaphore capacity")
}
#[cfg(feature = "http")]
pub(crate) fn invalid_download_url() -> Error {
    invalid("invalid blob URL")
}
#[cfg(feature = "http")]
pub(crate) fn download_scheme() -> Error {
    invalid("blob URL must use HTTP or HTTPS")
}

#[cfg(feature = "http")]
pub(crate) fn download_response(
    response: http::Response<opendal::HttpBody>,
) -> http::Response<opendal::HttpBody> {
    if response.status().is_success() {
        return response;
    }
    // OpenDAL collects error bodies for diagnostics. Discard an unbounded body.
    let (parts, _) = response.into_parts();
    http::Response::from_parts(
        parts,
        opendal::HttpBody::new(futures::stream::empty(), Some(0)),
    )
}
