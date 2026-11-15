use std::{num::NonZeroUsize, path::PathBuf, sync::Arc};

use open_arrow_sink_core::{
    CompressionCapability, DataWriter, Ordering, ParallelSink, Result, SchemaRef, Sink,
    SinkCapability, WriteOptions, WriteSession, Writer, async_trait, validation,
};
use parquet::arrow::AsyncArrowWriter;
use tokio::fs::{self, OpenOptions};

use crate::{
    committer::ParquetCommitter, destination::Destination, error::backend,
    writer::ParquetDirectoryWriter,
};

/// Writes a local directory containing Parquet files. Create is the default.
/// The parent directory must exist; overwrite stages output until commit.
/// Callers must exclude other writes or directory replacement during a session.
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
            supported_modes: &[
                open_arrow_sink_core::WriteMode::Create,
                open_arrow_sink_core::WriteMode::Append,
                open_arrow_sink_core::WriteMode::Overwrite,
            ],
            ordering: Ordering::Unordered,
            compression: CompressionCapability {
                global: crate::compression::SUPPORTED_CODECS,
                per_column: crate::compression::SUPPORTED_CODECS,
            },
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
        let destination = writers[0].destination.clone();
        Ok(WriteSession {
            writers: writers
                .into_iter()
                .map(|w| Box::new(w) as Box<dyn DataWriter<Output = u64>>)
                .collect(),
            committer: Box::new(ParquetCommitter(destination)),
        })
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
        validation::validate_options(&schema, &options, self.capability())?;
        let properties = crate::compression::writer_properties(&schema, &options.compression)?;
        let destination = Arc::new(
            Destination::open(directory, &schema, options.mode, options.cleanup_on_error).await?,
        );
        let directory = &destination.directory;
        let mut writers: Vec<ParquetDirectoryWriter> = Vec::new();
        for index in 0..count.get() {
            let path = directory.join(format!("{}{index:05}.parquet", destination.prefix));
            let opened = async {
                let file = OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(&path)
                    .await
                    .map_err(backend)?;
                match AsyncArrowWriter::try_new(file, schema.clone(), Some(properties.clone())) {
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
                    destination.clone(),
                    schema.clone(),
                    writer,
                    options.clone(),
                    properties.clone(),
                    index,
                    count.get(),
                )),
                Err(error) => {
                    for writer in writers {
                        let _ = Writer::abort(Box::new(writer)).await;
                    }
                    if options.cleanup_on_error && destination.remove_directory {
                        let _ = fs::remove_dir(&directory).await;
                    }
                    return Err(error);
                }
            }
        }
        Ok(writers)
    }
}
