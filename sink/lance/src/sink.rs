use std::{io, num::NonZeroUsize, path::PathBuf};

use open_arrow_sink_core::{
    CompressionCapability, CompressionCodec, DataWriter, IndexType, Ordering, ParallelSink, Result,
    SchemaRef, Sink, SinkCapability, WriteMode, WriteOptions, WriteSession, Writer, async_trait,
};

use crate::{
    compression::writer_schema,
    error::backend,
    validation::{validate_existing_dataset, validate_options, validate_path},
    writer::LanceWriter,
};

/// Writes a local dataset using native versioned create, append, or overwrite.
/// Create is the shared default. Only unordered writes are supported.
#[derive(Debug, Clone, Copy, Default)]
pub struct LanceDatasetSink;

#[async_trait]
impl Sink for LanceDatasetSink {
    type Request = PathBuf;

    fn capability(&self) -> SinkCapability {
        SinkCapability {
            supported_modes: &[WriteMode::Create, WriteMode::Append, WriteMode::Overwrite],
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
        let (path, uri) = prepare_destination(path, &encoding_schema, options.mode).await?;
        Ok(Box::new(LanceWriter::new(
            schema,
            encoding_schema,
            path,
            uri,
            options,
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
        let (path, uri) = prepare_destination(path, &encoding_schema, options.mode).await?;
        let writers = (0..count.get())
            .map(|_| {
                Box::new(crate::parallel::FragmentWriter::new(
                    schema.clone(),
                    encoding_schema.clone(),
                    uri.clone(),
                    &options,
                )) as Box<dyn DataWriter<Output = Self::Output>>
            })
            .collect();
        Ok(WriteSession {
            writers,
            committer: Box::new(crate::committer::LanceCommitter { path, uri, options }),
        })
    }
}

/// Only newly reserved directories belong to this session and may be removed.
async fn prepare_destination(
    path: PathBuf,
    encoding_schema: &SchemaRef,
    mode: WriteMode,
) -> Result<(Option<PathBuf>, String)> {
    // TODO: Use OpenDAL to support remote storage beyond local paths.
    let path = std::path::absolute(path).map_err(backend)?;
    let uri = validate_path(&path)?.to_owned();
    let reserved = match mode {
        WriteMode::Create => {
            tokio::fs::create_dir(&path).await.map_err(backend)?;
            true
        }
        WriteMode::Append => {
            validate_existing_dataset(&uri, Some(encoding_schema)).await?;
            false
        }
        WriteMode::Overwrite => match tokio::fs::create_dir(&path).await {
            Ok(()) => true,
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                // Never turn an unrelated existing directory into a dataset.
                validate_existing_dataset(&uri, None).await?;
                false
            }
            Err(error) => return Err(backend(error)),
        },
    };
    Ok((reserved.then_some(path), uri))
}
