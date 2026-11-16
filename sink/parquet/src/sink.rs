use open_arrow_sink_storage::{Storage, Url};
use std::{num::NonZeroUsize, sync::Arc};

use open_arrow_sink_core::{
    CompressionCapability, DataWriter, Ordering, ParallelSink, Result, SchemaRef, Sink,
    SinkCapability, WriteOptions, WriteSession, Writer, async_trait, validation,
};

use crate::{
    committer::ParquetCommitter, destination::Destination, writer::ParquetDirectoryWriter,
};

/// Writes a URL destination containing `part-00000.parquet`, etc.
/// Create and append are supported on every storage backend. Missing parents are created.
/// Callers must exclude concurrent writes or directory replacement during a session.
/// Directory scans have no promised row order, so use `Ordering::Unordered`.
/// Files become visible before commit; commit completes the footer, not an atomic
/// directory transaction. Drop does not commit or clean up incomplete files.
#[derive(Debug, Clone, Default)]
pub struct ParquetDirectorySink {
    storage: Option<Storage>,
}

impl ParquetDirectorySink {
    /// Reuse a configured OpenDAL operator, including credentials and layers.
    pub fn new(storage: Storage) -> Self {
        Self {
            storage: Some(storage),
        }
    }
}

#[async_trait]
impl Sink for ParquetDirectorySink {
    type Request = Url;

    fn capability(&self) -> SinkCapability {
        SinkCapability::default()
            .with_supported_modes(&[
                open_arrow_sink_core::WriteMode::Create,
                open_arrow_sink_core::WriteMode::Append,
            ])
            .with_ordering(Ordering::Unordered)
            .with_compression(
                CompressionCapability::default()
                    .with_global(crate::compression::SUPPORTED_CODECS)
                    .with_per_column(crate::compression::SUPPORTED_CODECS)
                    .with_structural_encodings(&[])
                    .with_nested_columns(false),
            )
            .with_supported_partitions(vec![])
            .with_supports_blob_handle(false)
            .with_supports_blob_streaming(false)
            .with_supports_upload_options(true)
            .with_supports_storage_version(false)
            .with_supported_indexes(&[])
    }

    async fn open(
        &self,
        directory: Url,
        schema: SchemaRef,
        options: WriteOptions,
    ) -> Result<Box<dyn Writer>> {
        self.open_writers(directory, schema, options, NonZeroUsize::MIN)
            .await?
            .into_writer()
            .await
    }
}

#[async_trait]
impl ParallelSink for ParquetDirectorySink {
    type Output = u64;

    async fn open_writers(
        &self,
        directory: Url,
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

impl ParquetDirectorySink {
    async fn create_writers(
        &self,
        directory: Url,
        schema: SchemaRef,
        options: WriteOptions,
        count: NonZeroUsize,
    ) -> Result<Vec<ParquetDirectoryWriter>> {
        validation::validate_options(&schema, &options, self.capability())?;
        let properties = crate::compression::writer_properties(&schema, &options.compression)?;
        let storage = match &self.storage {
            Some(storage) => storage.clone(),
            None => Storage::from_url(&directory)?,
        }
        .with_write_options(&options);
        let destination = Arc::new(
            Destination::open(
                storage,
                directory,
                &schema,
                options.mode,
                options.cleanup_on_error,
            )
            .await?,
        );
        let mut writers: Vec<ParquetDirectoryWriter> = Vec::new();
        for index in 0..count.get() {
            let opened = destination
                .open_writer(index, schema.clone(), properties.clone())
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
                        let _ = DataWriter::abort(Box::new(writer)).await;
                    }
                    if options.cleanup_on_error {
                        let _ = destination.remove_empty_directory().await;
                    }
                    return Err(error);
                }
            }
        }
        Ok(writers)
    }
}
