use std::{num::NonZeroUsize, path::PathBuf};

use open_arrow_sink_core::{
    Ordering, ParallelSink, Result, SchemaRef, Sink, SinkCapability, WriteOptions, Writer,
    async_trait, validation,
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
            supported_partitions: vec![],
            supported_indexes: &[],
        }
    }

    async fn open(
        &self,
        directory: PathBuf,
        schema: SchemaRef,
        options: WriteOptions,
    ) -> Result<Box<dyn Writer>> {
        let mut writers = self
            .open_writers(directory, schema, options, NonZeroUsize::new(1).unwrap())
            .await?;
        Ok(writers.pop().unwrap())
    }
}

#[async_trait]
impl ParallelSink for ParquetDirectorySink {
    async fn open_writers(
        &self,
        directory: PathBuf,
        schema: SchemaRef,
        options: WriteOptions,
        count: NonZeroUsize,
    ) -> Result<Vec<Box<dyn Writer>>> {
        validation::validate_options(&options, self.capability())?;
        fs::create_dir(&directory).await.map_err(backend)?;
        let mut writers: Vec<Box<dyn Writer>> = Vec::new();
        for index in 0..count.get() {
            let path = directory.join(format!("part-{index:05}.parquet"));
            let opened = async {
                let file = OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(&path)
                    .await
                    .map_err(backend)?;
                match AsyncArrowWriter::try_new(file, schema.clone(), None) {
                    Ok(writer) => Ok(writer),
                    Err(error) => {
                        if options.cleanup_on_error {
                            let _ = fs::remove_file(&path).await;
                        }
                        Err(backend(error))
                    }
                }
            }
            .await;
            match opened {
                Ok(writer) => writers.push(Box::new(ParquetDirectoryWriter::new(
                    directory.clone(),
                    schema.clone(),
                    writer,
                    options.clone(),
                    index,
                    count.get(),
                ))),
                Err(error) => {
                    for writer in writers {
                        let _ = writer.abort().await;
                    }
                    if options.cleanup_on_error {
                        let _ = fs::remove_dir(&directory).await;
                    }
                    return Err(error);
                }
            }
        }
        Ok(writers)
    }
}
