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
) -> opendal::Result<http::Response<opendal::HttpBody>> {
    if matches!(response.status().as_u16(), 401 | 407) {
        return Err(opendal::Error::new(
            ErrorKind::PermissionDenied,
            "source authentication failed",
        )
        .set_source(crate::SourceDownloadError::new(
            crate::DownloadFailureScope::Configuration,
            Error::InvalidConfig(format!("HTTP source returned {}", response.status())),
        )));
    }
    if response.status().is_success() {
        return Ok(response);
    }
    // OpenDAL collects error bodies for diagnostics. Discard an unbounded body.
    let (parts, _) = response.into_parts();
    Ok(http::Response::from_parts(
        parts,
        opendal::HttpBody::new(futures::stream::empty(), Some(0)),
    ))
}
