use open_arrow_sink_core::Error;

pub(crate) fn invalid_session() -> Error {
    Error::Unsupported("backend returned an invalid parallel writer session".into())
}

pub(crate) fn download_context(column: &str, row: usize, source: Error) -> Error {
    Error::backend(DownloadError {
        column: column.into(),
        row,
        source,
    })
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

pub(crate) fn streaming_blobs_required() -> Error {
    Error::Unsupported("download layer requires a sink with streaming blob support".into())
}
pub(crate) fn download_column_unique(column: &str) -> Error {
    Error::Unsupported(format!(
        "download column must exist and be unique: {column}"
    ))
}
pub(crate) fn download_column_type(column: &str) -> Error {
    Error::Unsupported(format!("download column must be a string: {column}"))
}
#[cfg(test)]
mod tests {
    use super::*;
    use std::{error::Error as _, io};

    #[test]
    fn download_context_preserves_underlying_source() {
        let error = download_context(
            "image",
            7,
            Error::backend(io::Error::from(io::ErrorKind::TimedOut)),
        );
        assert!(error.to_string().contains("download column image row 7"));
        let native = error
            .source()
            .unwrap()
            .source()
            .unwrap()
            .source()
            .unwrap()
            .downcast_ref::<io::Error>()
            .unwrap();
        assert_eq!(native.kind(), io::ErrorKind::TimedOut);
    }
}
