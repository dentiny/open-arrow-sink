use open_arrow_sink_core::{Error, Result};
use opendal::ErrorKind;

/// Preserve the operation, object location, and original OpenDAL error.
pub(crate) fn map_error(operation: &'static str, path: &str, source: opendal::Error) -> Error {
    Error::backend(BackendError {
        operation,
        path: path.to_owned(),
        source,
    })
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
        // handle stops this stream; the owning sink deletes its files.
        Err(error) if error.kind() == ErrorKind::Unsupported => Ok(()),
        Err(error) => Err(map_error("abort", path, error)),
    }
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
