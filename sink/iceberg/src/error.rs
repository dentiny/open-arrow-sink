pub(crate) fn storage_backend(
    error: impl std::error::Error + Send + Sync + 'static,
) -> iceberg::Error {
    iceberg::Error::new(
        iceberg::ErrorKind::Unexpected,
        "OpenDAL storage operation failed",
    )
    .with_source(error)
}
