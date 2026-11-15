//! Destination validation before creating session-owned output.
use std::{io, path::Path};

use open_arrow_sink_core::{Error, Result, SchemaRef};
use parquet::arrow::ParquetRecordBatchStreamBuilder;
use tokio::fs;

use crate::error::backend;

pub(crate) async fn validate_append_directory(path: &Path, schema: &SchemaRef) -> Result<()> {
    let mut entries = fs::read_dir(path).await.map_err(backend)?;
    while let Some(entry) = entries.next_entry().await.map_err(backend)? {
        if entry.path().extension().is_some_and(|ext| ext == "parquet") {
            let file = fs::File::open(entry.path()).await.map_err(backend)?;
            let reader = ParquetRecordBatchStreamBuilder::new(file)
                .await
                .map_err(backend)?;
            if reader.schema() != schema {
                return Err(Error::SchemaMismatch);
            }
        }
    }
    Ok(())
}

pub(crate) async fn validate_overwrite_directory(path: &Path) -> Result<()> {
    match fs::symlink_metadata(path).await {
        Ok(metadata) if !metadata.is_dir() => Err(backend(io::Error::other(
            "destination must be a directory, not a file or symlink",
        ))),
        Ok(_) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(backend(error)),
    }
}

pub(crate) fn validate_writer_state<T>(writer: Option<T>) -> Result<T> {
    writer.ok_or_else(crate::error::closed)
}
