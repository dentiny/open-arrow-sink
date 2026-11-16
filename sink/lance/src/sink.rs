use open_arrow_sink_storage::{Storage, StorageResolver, Url};
use std::num::NonZeroUsize;

use open_arrow_sink_core::{
    CompressionCapability, CompressionCodec, DataWriter, Ordering, ParallelSink, Result, SchemaRef,
    Sink, SinkCapability, WriteMode, WriteOptions, WriteSession, Writer, async_trait,
};

use crate::{storage::NativeStorage, validation::validate_options, write_options};

/// Creates, appends to, or version-overwrites a URL dataset using native Lance commits.
/// Only unordered writes are supported; dataset scan order is not guaranteed.
#[derive(Debug, Clone, Default)]
pub struct LanceDatasetSink {
    storage: Option<Storage>,
    sources: StorageResolver,
    options: crate::LanceOptions,
}

impl LanceDatasetSink {
    pub fn new(storage: Storage) -> Self {
        Self {
            storage: Some(storage),
            sources: StorageResolver::default(),
            options: crate::LanceOptions::default(),
        }
    }
    /// Configure backend-specific options before applying reusable layers.
    pub fn with_options(mut self, options: crate::LanceOptions) -> Self {
        self.options = options;
        self
    }
    /// Configure an independent source namespace for native external blob ingestion.
    pub fn with_source_storage(mut self, storage: Storage) -> Self {
        self.sources = self.sources.with_storage(storage);
        self
    }
    fn resolve(
        &self,
        url: Url,
        metrics: Option<&open_arrow_sink_core::IoMetrics>,
    ) -> Result<NativeStorage> {
        let storage = match &self.storage {
            Some(storage) => storage.clone(),
            None => Storage::from_url(&url)?,
        };
        NativeStorage::new(storage, url, self.sources.clone(), metrics)
    }
    /// Configure native Lance reads with the same shared operator as writes.
    pub fn dataset_builder(&self, url: Url) -> Result<lance::dataset::builder::DatasetBuilder> {
        Ok(self.resolve(url, None)?.dataset_builder())
    }
}

#[async_trait]
impl Sink for LanceDatasetSink {
    type Request = Url;

    fn capability(&self) -> SinkCapability {
        SinkCapability::default()
            .with_supported_modes(&[WriteMode::Create, WriteMode::Append, WriteMode::Overwrite])
            .with_ordering(Ordering::Unordered)
            .with_compression(
                CompressionCapability::default()
                    .with_global(&[CompressionCodec::Uncompressed, CompressionCodec::Zstd])
                    .with_per_column(&[CompressionCodec::Uncompressed, CompressionCodec::Zstd])
                    .with_structural_encodings(&[
                        open_arrow_sink_core::StructuralEncoding::Miniblock,
                        open_arrow_sink_core::StructuralEncoding::Fullzip,
                    ])
                    .with_nested_columns(true),
            )
            .with_supports_blob_streaming(true)
    }

    async fn open(
        &self,
        url: Url,
        schema: SchemaRef,
        options: WriteOptions,
    ) -> Result<Box<dyn Writer>> {
        let storage_schema = crate::blob::storage_schema(&schema)?;
        if storage_schema != schema {
            let options = self.options.session(options);
            validate_options(&schema, &options, self.capability())?;
            let encoding_schema = write_options::writer_schema(&schema, &options)?;
            let mut destination = self.resolve(url, options.io_metrics.as_ref())?;
            destination
                .prepare(
                    &crate::blob::logical_schema(&encoding_schema),
                    options.mode,
                    options.storage_version,
                )
                .await?;
            destination.validate_index_requests(&options).await?;
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
        let options = self.options.session(options);
        validate_options(&schema, &options, self.capability())?;
        crate::validation::validate_parallel_blob_schema(&schema)?;
        let encoding_schema = write_options::writer_schema(&schema, &options)?;
        let mut destination = self.resolve(url, options.io_metrics.as_ref())?;
        destination
            .prepare(&encoding_schema, options.mode, options.storage_version)
            .await?;
        destination.validate_index_requests(&options).await?;
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
