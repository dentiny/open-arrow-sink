use open_arrow_sink_storage::{Storage, Url};
use std::num::NonZeroUsize;

use open_arrow_sink_core::{
    CompressionCapability, CompressionCodec, DataWriter, IndexType, Ordering, ParallelSink, Result,
    SchemaRef, Sink, SinkCapability, WriteMode, WriteOptions, WriteSession, Writer, async_trait,
};

use crate::{storage::NativeStorage, validation::validate_options, write_options};

/// Creates, appends to, or version-overwrites a URL dataset using native Lance commits.
/// Only unordered writes are supported; dataset scan order is not guaranteed.
#[derive(Debug, Clone, Default)]
pub struct LanceDatasetSink {
    storage: Option<Storage>,
}

impl LanceDatasetSink {
    pub fn new(storage: Storage) -> Self {
        Self {
            storage: Some(storage),
        }
    }
    fn resolve(&self, url: Url) -> Result<NativeStorage> {
        let storage = match &self.storage {
            Some(storage) => storage.clone(),
            None => Storage::from_url(&url)?,
        };
        NativeStorage::new(storage, url)
    }
    /// Configure native Lance reads with the same shared operator as writes.
    pub fn dataset_builder(&self, url: Url) -> Result<lance::dataset::builder::DatasetBuilder> {
        Ok(self.resolve(url)?.dataset_builder())
    }
}

#[async_trait]
impl Sink for LanceDatasetSink {
    type Request = Url;

    fn capability(&self) -> SinkCapability {
        SinkCapability {
            supported_modes: &[WriteMode::Create, WriteMode::Append, WriteMode::Overwrite],
            ordering: Ordering::Unordered,
            compression: CompressionCapability {
                global: &[CompressionCodec::Uncompressed, CompressionCodec::Zstd],
                per_column: &[CompressionCodec::Uncompressed, CompressionCodec::Zstd],
                structural_encodings: &[
                    open_arrow_sink_core::StructuralEncoding::Miniblock,
                    open_arrow_sink_core::StructuralEncoding::Fullzip,
                ],
                nested_columns: true,
            },
            supported_partitions: vec![],
            supports_blob_handle: true,
            supports_blob_streaming: true,
            supports_storage_version: true,
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
        url: Url,
        schema: SchemaRef,
        options: WriteOptions,
    ) -> Result<Box<dyn Writer>> {
        let storage_schema = crate::blob::storage_schema(&schema)?;
        if storage_schema != schema {
            validate_options(&schema, &options, self.capability())?;
            let encoding_schema = write_options::writer_schema(&schema, &options)?;
            let mut destination = self.resolve(url)?;
            destination
                .prepare(&crate::blob::logical_schema(&encoding_schema), options.mode)
                .await?;
            return Ok(Box::new(crate::blob_writer::BlobDatasetWriter::new(
                schema,
                storage_schema,
                encoding_schema,
                destination,
                options,
            )?));
        }
        // Use the same fragment writing and publication path for every session.
        self.open_writers(url, schema, options, NonZeroUsize::MIN)
            .await?
            .into_writer()
            .await
    }
}

#[async_trait]
impl ParallelSink for LanceDatasetSink {
    type Output = (u64, lance::dataset::transaction::Transaction);
    async fn open_writers(
        &self,
        url: Url,
        schema: SchemaRef,
        options: WriteOptions,
        count: NonZeroUsize,
    ) -> Result<WriteSession<Self::Output>> {
        validate_options(&schema, &options, self.capability())?;
        crate::validation::validate_parallel_blob_schema(&schema)?;
        let encoding_schema = write_options::writer_schema(&schema, &options)?;
        let mut destination = self.resolve(url)?;
        destination.prepare(&encoding_schema, options.mode).await?;
        let writers = (0..count.get())
            .map(|_| {
                Ok(Box::new(crate::parallel::FragmentWriter::new(
                    schema.clone(),
                    encoding_schema.clone(),
                    destination.clone(),
                    &options,
                )?)
                    as Box<dyn DataWriter<Output = Self::Output>>)
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(WriteSession {
            writers,
            committer: Box::new(crate::committer::LanceCommitter {
                destination,
                options,
            }),
        })
    }
}
