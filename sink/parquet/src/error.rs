use open_arrow_sink_core::Error;

pub(crate) fn parquet_backend(error: Error) -> parquet::errors::ParquetError {
    parquet::errors::ParquetError::External(Box::new(error))
}
