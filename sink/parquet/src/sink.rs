use std::path::PathBuf;

use open_arrow_sink_core::{
    Ordering, Result, SchemaRef, Sink, SinkCapability, WriteOptions, Writer, async_trait,
    validation,
};
use parquet::arrow::AsyncArrowWriter;
use tokio::fs::{self, OpenOptions};

use crate::{error::backend, writer::ParquetDirectoryWriter};

/// Creates a new local directory containing `part-00000.parquet`, etc.
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
            partition: false,
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
                if options.cleanup_on_error {
                    let _ = fs::remove_dir(&directory).await;
                }
                return Err(backend(error));
            }
        };
        let writer = match AsyncArrowWriter::try_new(file, schema.clone(), None) {
            Ok(writer) => writer,
            Err(error) => {
                if options.cleanup_on_error {
                    let _ = fs::remove_file(&path).await;
                    let _ = fs::remove_dir(&directory).await;
                }
                return Err(backend(error));
            }
        };
        Ok(Box::new(ParquetDirectoryWriter::new(
            directory, schema, writer, options,
        )))
    }
}
