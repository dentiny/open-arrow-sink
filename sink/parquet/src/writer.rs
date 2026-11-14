use std::{io, path::PathBuf};

use open_arrow_sink_core::{
    RecordBatch, Result, SchemaRef, WriteSummary, Writer, async_trait, validation,
};
use parquet::arrow::AsyncArrowWriter;
use tokio::fs::{self, File};

use crate::error::{backend, closed};

pub(crate) struct ParquetDirectoryWriter {
    directory: PathBuf,
    schema: SchemaRef,
    writer: Option<AsyncArrowWriter<File>>,
    rows_written: u64,
}

#[async_trait]
impl Writer for ParquetDirectoryWriter {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    async fn write(&mut self, batch: RecordBatch) -> Result<()> {
        // Taking ownership also makes a failed or cancelled write terminal.
        let mut writer = self.writer.take().ok_or_else(closed)?;
        validation::validate_batch(&self.schema, &batch)?;
        writer.write(&batch).await.map_err(backend)?;
        // Flush each batch rather than retaining the complete input in memory.
        writer.flush().await.map_err(backend)?;
        self.rows_written += batch.num_rows() as u64;
        self.writer = Some(writer);
        Ok(())
    }

    async fn commit(mut self: Box<Self>) -> Result<WriteSummary> {
        let writer = self.writer.take().ok_or_else(closed)?;
        if let Err(error) = writer.close().await {
            let _ = self.cleanup().await;
            return Err(backend(error));
        }
        Ok(WriteSummary {
            rows_written: self.rows_written,
        })
    }

    async fn abort(mut self: Box<Self>) -> Result<()> {
        drop(self.writer.take());
        self.cleanup().await.map_err(backend)
    }
}

impl ParquetDirectoryWriter {
    pub(crate) fn new(
        directory: PathBuf,
        schema: SchemaRef,
        writer: AsyncArrowWriter<File>,
    ) -> Self {
        Self {
            directory,
            schema,
            writer: Some(writer),
            rows_written: 0,
        }
    }

    async fn cleanup(&mut self) -> io::Result<()> {
        let path = self.directory.join("part-00000.parquet");
        match fs::remove_file(path).await {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
        // Never recursively delete a directory: other files may have been added.
        fs::remove_dir(&self.directory).await
    }
}
