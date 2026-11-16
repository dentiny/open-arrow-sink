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
        // handle stops this stream; the owning sink deletes its files.
        Err(error) if error.kind() == ErrorKind::Unsupported => Ok(()),
        Err(error) => Err(map_error("abort", path, error)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::error::Error as _;

    #[test]
    fn backend_errors_retain_operation_path_and_native_source() {
        let error = map_error(
            "read",
            "object",
            opendal::Error::new(ErrorKind::Unexpected, "transport failed"),
        );
        assert!(matches!(error, Error::Backend(_)));
        assert!(error.to_string().contains("read object \"object\""));
        let source = error.source().unwrap().source().unwrap();
        assert_eq!(
            source.downcast_ref::<opendal::Error>().unwrap().kind(),
            ErrorKind::Unexpected
        );
    }

    #[test]
    fn unsupported_operations_remain_classified() {
        let error = map_error(
            "create",
            "object",
            opendal::Error::new(ErrorKind::Unsupported, "conditional writes unavailable"),
        );
        assert!(matches!(error, Error::Unsupported(_)));
        assert!(error.to_string().contains("create object \"object\""));
    }
}
