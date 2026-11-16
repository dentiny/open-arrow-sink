use open_arrow_sink_core::Error;

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
