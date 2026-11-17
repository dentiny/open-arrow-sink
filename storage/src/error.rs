use open_arrow_sink_core::{Error, Result};
use opendal::ErrorKind;

/// Classify actionable errors; retain the native source for backend failures.
pub(crate) fn map_error(operation: &'static str, path: &str, source: opendal::Error) -> Error {
    let kind = source.kind();
    let context = BackendError {
        operation,
        path: path.to_owned(),
        source,
    };
    match kind {
        ErrorKind::AlreadyExists => Error::AlreadyExists(context.to_string()),
        ErrorKind::NotFound => Error::NotFound(context.to_string()),
        ErrorKind::Unsupported => Error::Unsupported(context.to_string()),
        _ => Error::backend(context),
    }
}

#[derive(Debug, thiserror::Error)]
#[error("{operation} object {path:?}: {source}")]
struct BackendError {
    operation: &'static str,
    path: String,
    #[source]
    source: opendal::Error,
}

pub(crate) fn delete_result(path: &str, result: opendal::Result<()>) -> Result<()> {
    match result {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(()),
        Err(error) => Err(map_error("delete", path, error)),
    }
}

pub(crate) fn abort_result(path: &str, result: opendal::Result<()>) -> Result<()> {
    match result {
        Ok(()) => Ok(()),
        // A backend without abort has no pending upload to cancel. Dropping the
        // handle stops this stream; persisted output remains in place.
        Err(error) if error.kind() == ErrorKind::Unsupported => Ok(()),
        Err(error) => Err(map_error("abort", path, error)),
    }
}

pub(crate) fn download_limit() -> Error {
    Error::InvalidConfig("download limits exceed semaphore capacity".into())
}
pub(crate) fn invalid_download_url() -> Error {
    Error::InvalidInput("invalid blob URL".into())
}
pub(crate) fn download_scheme() -> Error {
    Error::InvalidInput("blob URL must use HTTP or HTTPS".into())
}
pub(crate) fn http_storage_service() -> Error {
    Error::InvalidConfig("HTTP download storage must use the OpenDAL http service".into())
}

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

pub(crate) fn download_payload_limit() -> Error {
    Error::InvalidConfig("download payload and chunk limits must fit the shared byte budget".into())
}
pub(crate) fn download_too_large(limit: usize) -> Error {
    Error::InvalidInput(format!(
        "download group exceeds its {limit}-byte payload limit"
    ))
}
