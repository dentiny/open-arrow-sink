use open_arrow_sink_core::Error;

pub(crate) fn download_context(column: &str, row: usize, source: Error) -> Error {
    let context = |message| format!("download column {column} row {row}: {message}");
    match source {
        Error::AlreadyExists(message) => Error::AlreadyExists(context(message)),
        Error::NotFound(message) => Error::NotFound(context(message)),
        Error::Unsupported(message) => Error::Unsupported(context(message)),
        Error::InvalidInput(message) => Error::InvalidInput(context(message)),
        Error::InvalidConfig(message) => Error::InvalidConfig(context(message)),
        source @ Error::Backend(_) => Error::backend(DownloadError {
            column: column.into(),
            row,
            source,
        }),
        source => source,
    }
}

#[derive(Debug)]
struct DownloadError {
    column: String,
    row: usize,
    source: Error,
}
impl std::fmt::Display for DownloadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "download column {} row {}: {}",
            self.column, self.row, self.source
        )
    }
}
impl std::error::Error for DownloadError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.source)
    }
}

/// Translate native download failures before adding column and row context.
pub(crate) fn download_error(source: open_arrow_sink_storage::opendal::Error) -> Error {
    use open_arrow_sink_storage::opendal::ErrorKind;
    match source.kind() {
        ErrorKind::AlreadyExists => Error::AlreadyExists(source.to_string()),
        ErrorKind::NotFound => Error::NotFound(source.to_string()),
        ErrorKind::Unsupported => Error::Unsupported(source.to_string()),
        _ => Error::backend(source),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn download_context_preserves_categories() {
        let errors = [
            Error::AlreadyExists("object".into()),
            Error::NotFound("object".into()),
            Error::Unsupported("operation".into()),
            Error::InvalidInput("URL".into()),
            Error::InvalidConfig("limit".into()),
        ];
        for error in errors {
            let kind = std::mem::discriminant(&error);
            let error = download_context("blob", 3, error);
            assert_eq!(std::mem::discriminant(&error), kind);
            assert!(error.to_string().contains("download column blob row 3"));
        }
        assert!(matches!(
            download_context("blob", 3, Error::Poisoned),
            Error::Poisoned
        ));
    }

    #[test]
    fn backend_download_retains_native_source() {
        use open_arrow_sink_storage::opendal::{Error as NativeError, ErrorKind};
        use std::error::Error as _;

        let error = download_context(
            "blob",
            3,
            download_error(NativeError::new(ErrorKind::Unexpected, "transport failed")),
        );
        assert!(matches!(error, Error::Backend(_)));
        assert!(error.to_string().contains("download column blob row 3"));
        let mut source = error.source();
        while let Some(cause) = source {
            if let Some(native) = cause.downcast_ref::<NativeError>() {
                assert_eq!(native.kind(), ErrorKind::Unexpected);
                return;
            }
            source = cause.source();
        }
        panic!("native download error must remain in the source chain");
    }
}
