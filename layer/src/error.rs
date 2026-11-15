use open_arrow_sink_core::Error;

pub(crate) fn unsupported() -> Error {
    Error::Unsupported("writer failed or was cancelled; abort or drop it".into())
}
pub(crate) fn invalid_session() -> Error {
    Error::Unsupported("backend returned an invalid parallel writer session".into())
}

pub(crate) fn backend(error: impl std::error::Error + Send + Sync + 'static) -> Error {
    Error::Backend(Box::new(error))
}
pub(crate) fn download_context(column: &str, row: usize, error: Error) -> Error {
    backend(std::io::Error::other(format!(
        "download column {column} row {row}: {error}"
    )))
}
pub(crate) fn download_failed() -> Error {
    backend(std::io::Error::other(
        "blob writer failed; abort or drop it",
    ))
}
pub(crate) fn download_aborted() -> Error {
    backend(std::io::Error::other("blob writer failed; session aborted"))
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
