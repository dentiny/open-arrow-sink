use open_arrow_sink_core::{Error, ZSTD_COMPRESSION_LEVEL_RANGE};

pub(crate) fn backend(error: impl std::error::Error + Send + Sync + 'static) -> Error {
    Error::Backend(Box::new(error))
}

pub(crate) fn invalid_compression_level(level: i32) -> Error {
    Error::Unsupported(format!(
        "Lance Zstd compression level must be between 0 and {}: {level}",
        ZSTD_COMPRESSION_LEVEL_RANGE.end(),
    ))
}
