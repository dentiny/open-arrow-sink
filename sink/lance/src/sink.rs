use std::{num::NonZeroUsize, path::PathBuf};

use open_arrow_sink_core::{
    CompressionCapability, CompressionCodec, DataWriter, IndexType, Ordering, ParallelSink, Result,
    SchemaRef, Sink, SinkCapability, WriteOptions, WriteSession, Writer, async_trait,
};

use crate::{
    compression::writer_schema,
    error::{backend, invalid_path},
    validation::validate_options,
    writer::LanceWriter,
};

/// Creates a new local dataset directory. Existing paths are always rejected.
/// Only unordered writes are supported; dataset scan order is not guaranteed.
#[derive(Debug, Clone, Copy, Default)]
pub struct LanceDatasetSink;

#[async_trait]
impl Sink for LanceDatasetSink {
    type Request = PathBuf;

    fn capability(&self) -> SinkCapability {
        SinkCapability {
            ordering: Ordering::Unordered,
            compression: CompressionCapability {
                global: &[CompressionCodec::Uncompressed, CompressionCodec::Zstd],
                per_column: &[CompressionCodec::Uncompressed, CompressionCodec::Zstd],
            },
            supported_partitions: vec![],
            supported_indexes: &[
                IndexType::Scalar,
                IndexType::Vector,
                IndexType::Text,
                IndexType::BloomFilter,
            ],
        }
    }

    async fn open(
        &self,
        path: PathBuf,
        schema: SchemaRef,
        options: WriteOptions,
    ) -> Result<Box<dyn Writer>> {
        validate_options(&schema, &options, self.capability())?;
        let encoding_schema = writer_schema(&schema, &options.compression);
        // Reserve a fresh destination. The caller must keep exclusive ownership of
        // this directory until commit/abort; no existing dataset is overwritten.
        // TODO: Use OpenDAL to support remote storage beyond local paths.
        let path = std::path::absolute(path).map_err(backend)?;
        let uri = path.to_str().ok_or_else(invalid_path)?.to_owned();
        tokio::fs::create_dir(&path).await.map_err(backend)?;
        Ok(Box::new(LanceWriter::new(
            schema,
            encoding_schema,
            path,
            uri,
            options.target_file_size,
            options.indexes,
        )))
    }
}

#[async_trait]
impl ParallelSink for LanceDatasetSink {
    type Output = (u64, lance::dataset::transaction::Transaction);
    async fn open_writers(
        &self,
        path: PathBuf,
        schema: SchemaRef,
        options: WriteOptions,
        count: NonZeroUsize,
    ) -> Result<WriteSession<Self::Output>> {
        validate_options(&schema, &options, self.capability())?;
        let encoding_schema = writer_schema(&schema, &options.compression);
        let path = std::path::absolute(path).map_err(backend)?;
        let uri = path.to_str().ok_or_else(invalid_path)?.to_owned();
        tokio::fs::create_dir(&path).await.map_err(backend)?;
        let writers = (0..count.get())
            .map(|_| {
                Box::new(crate::parallel::FragmentWriter::new(
                    schema.clone(),
                    encoding_schema.clone(),
                    uri.clone(),
                    options.target_file_size,
                )) as Box<dyn DataWriter<Output = Self::Output>>
            })
            .collect();
        Ok(WriteSession {
            writers,
            committer: Box::new(crate::committer::LanceCommitter {
                path,
                uri,
                indexes: options.indexes,
                cleanup_on_error: options.cleanup_on_error,
            }),
        })
    }
}
