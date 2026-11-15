use std::{io, path::PathBuf};

use open_arrow_sink_core::{
    CompressionCapability, CompressionCodec, IndexType, Ordering, Result, SchemaRef, Sink,
    SinkCapability, WriteMode, WriteOptions, Writer, async_trait,
};

use crate::{
    compression::writer_schema, error::backend, validation::validate_options, writer::LanceWriter,
};

/// Writes a local dataset using native versioned create, append, or overwrite.
/// Only unordered writes are supported; dataset scan order is not guaranteed.
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
        // Newly created directories are session-owned; existing datasets must
        // never be recursively removed by abort.
        // TODO: Use OpenDAL to support remote storage beyond local paths.
        let path = std::path::absolute(path).map_err(backend)?;
        let uri = path
            .to_str()
            .ok_or_else(|| {
                backend(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "Lance requires a UTF-8 path",
                ))
            })?
            .to_owned();
        let mode = options.mode.unwrap_or(WriteMode::Create);
        let reserved = match mode {
            WriteMode::Create => {
                tokio::fs::create_dir(&path).await.map_err(backend)?;
                true
            }
            WriteMode::Append => {
                let dataset = lance::Dataset::open(&uri).await.map_err(backend)?;
                if arrow_schema::Schema::from(dataset.schema()) != *encoding_schema {
                    return Err(open_arrow_sink_core::Error::SchemaMismatch);
                }
                false
            }
            WriteMode::Overwrite => {
                match tokio::fs::create_dir(&path).await {
                    Ok(()) => true,
                    Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                        // Do not turn an arbitrary pre-existing directory into a dataset.
                        lance::Dataset::open(&uri).await.map_err(backend)?;
                        false
                    }
                    Err(error) => return Err(backend(error)),
                }
            }
        };
        Ok(Box::new(LanceWriter::new(
            schema,
            encoding_schema,
            reserved.then_some(path),
            uri,
            options,
        )))
    }
}
