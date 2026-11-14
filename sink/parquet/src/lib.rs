//! Create-only local directory sink: one uncompressed Parquet file per session.
use std::{io, path::PathBuf};

use open_arrow_sink_core::{
    Error, Ordering, RecordBatch, Result, SchemaRef, Sink, SinkCapability, WriteOptions,
    WriteSummary, Writer, async_trait, validation,
};
use parquet::arrow::AsyncArrowWriter;
use tokio::fs::{self, File, OpenOptions};

/// Creates a new local directory containing `part-00000.parquet`.
/// The parent directory must already exist. Existing destinations are never replaced.
/// Directory scans have no promised row order, so use `Ordering::Unordered`.
/// Files become visible before commit; commit completes the footer, not an atomic
/// directory transaction. Drop does not commit or clean up incomplete files.
#[derive(Debug, Clone, Copy, Default)]
pub struct ParquetDirectorySink;

#[async_trait]
impl Sink for ParquetDirectorySink {
    type Request = PathBuf;

    fn capability(&self) -> SinkCapability {
        SinkCapability {
            ordering: Ordering::Unordered,
        }
    }

    async fn open(
        &self,
        directory: PathBuf,
        schema: SchemaRef,
        options: WriteOptions,
    ) -> Result<Box<dyn Writer>> {
        validation::validate_options(&options, self.capability())?;
        fs::create_dir(&directory).await.map_err(backend)?;
        let path = directory.join("part-00000.parquet");
        let file = match OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .await
        {
            Ok(file) => file,
            Err(error) => {
                let _ = fs::remove_dir(&directory).await;
                return Err(backend(error));
            }
        };
        let writer = match AsyncArrowWriter::try_new(file, schema.clone(), None) {
            Ok(writer) => writer,
            Err(error) => {
                let _ = fs::remove_file(&path).await;
                let _ = fs::remove_dir(&directory).await;
                return Err(backend(error));
            }
        };
        Ok(Box::new(ParquetDirectoryWriter {
            directory,
            schema,
            writer: Some(writer),
            rows_written: 0,
        }))
    }
}

struct ParquetDirectoryWriter {
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

fn backend(error: impl std::error::Error + Send + Sync + 'static) -> Error {
    Error::Backend(Box::new(error))
}

fn closed() -> Error {
    backend(io::Error::other("writer is no longer active"))
}
