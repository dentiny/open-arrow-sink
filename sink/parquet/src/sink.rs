use std::{num::NonZeroUsize, path::PathBuf};

use open_arrow_sink_core::{
    DataWriter, Ordering, ParallelSink, Result, SchemaRef, SessionCommitter, Sink, SinkCapability,
    WriteOptions, WriteSession, WriteSummary, Writer, async_trait, validation,
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
            .create_writers(directory, schema, options, NonZeroUsize::new(1).unwrap())
            .await?;
        Ok(Box::new(writers.pop().unwrap()))
    }
}

#[async_trait]
impl ParallelSink for ParquetDirectorySink {
    type Output = u64;

    async fn open_writers(
        &self,
        directory: PathBuf,
        schema: SchemaRef,
        options: WriteOptions,
        count: NonZeroUsize,
    ) -> Result<WriteSession<Self::Output>> {
        let writers = self
            .create_writers(directory, schema, options, count)
            .await?;
        Ok(WriteSession {
            writers: writers
                .into_iter()
                .map(|w| Box::new(w) as Box<dyn DataWriter<Output = u64>>)
                .collect(),
            committer: Box::new(ParquetCommitter),
        })
    }
}

struct ParquetCommitter;

#[async_trait]
impl SessionCommitter<u64> for ParquetCommitter {
    async fn commit(self: Box<Self>, outputs: Vec<u64>) -> Result<WriteSummary> {
        // Parquet has no metadata transaction; finish already closed each footer.
        Ok(WriteSummary {
            rows_written: outputs.into_iter().sum(),
        })
    }
    async fn abort(self: Box<Self>) -> Result<()> {
        Ok(())
    }
}

impl ParquetDirectorySink {
    async fn create_writers(
        &self,
        directory: PathBuf,
        schema: SchemaRef,
        options: WriteOptions,
        count: NonZeroUsize,
    ) -> Result<Vec<ParquetDirectoryWriter>> {
        validation::validate_options(&options, self.capability())?;
        fs::create_dir(&directory).await.map_err(backend)?;
        let mut writers: Vec<ParquetDirectoryWriter> = Vec::new();
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
                Ok(writer) => writers.push(ParquetDirectoryWriter::new(
                    directory.clone(),
                    schema.clone(),
                    writer,
                    options.clone(),
                    index,
                    count.get(),
                )),
                Err(error) => {
                    for writer in writers {
                        let _ = Writer::abort(Box::new(writer)).await;
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
