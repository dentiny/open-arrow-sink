use open_arrow_sink_storage::{Storage, Url};
use std::num::NonZeroUsize;

use open_arrow_sink_core::{
    CompressionCapability, CompressionCodec, DataWriter, IndexType, Ordering, ParallelSink, Result,
    SchemaRef, Sink, SinkCapability, WriteOptions, WriteSession, Writer, async_trait,
};

use crate::{
    compression::writer_schema, storage::NativeStorage, validation::validate_options,
    writer::LanceWriter,
};

/// Creates a new URL dataset. Existing destinations are always rejected.
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
            supported_modes: &[open_arrow_sink_core::WriteMode::Create],
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
        url: Url,
        schema: SchemaRef,
        options: WriteOptions,
    ) -> Result<Box<dyn Writer>> {
        validate_options(&schema, &options, self.capability())?;
        let encoding_schema = writer_schema(&schema, &options.compression);
        let destination = self.resolve(url)?;
        destination.reserve().await?;
        Ok(Box::new(LanceWriter::new(
            schema,
            encoding_schema,
            destination,
            options,
        )))
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
        let encoding_schema = writer_schema(&schema, &options.compression);
        let destination = self.resolve(url)?;
        destination.reserve().await?;
        let writers = (0..count.get())
            .map(|_| {
                Box::new(crate::parallel::FragmentWriter::new(
                    schema.clone(),
                    encoding_schema.clone(),
                    destination.clone(),
                    options.target_file_size,
                )) as Box<dyn DataWriter<Output = Self::Output>>
            })
            .collect();
        Ok(WriteSession {
            writers,
            committer: Box::new(crate::committer::LanceCommitter {
                destination,
                options,
            }),
        })
    }
}
