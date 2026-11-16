use open_arrow_sink_storage::{Storage, StorageResolver, Url};
use std::{num::NonZeroUsize, sync::Arc};

use open_arrow_sink_core::{
    CompressionCapability, CompressionCodec, DataWriter, IndexType, Ordering, ParallelSink, Result,
    SchemaRef, Sink, SinkCapability, WriteMode, WriteOptions, WriteSession, Writer, async_trait,
};

use crate::{
    LanceFragments, destination::LanceDestination, validation::validate_options, write_options,
};

/// Creates, appends to, or version-overwrites a URL dataset using native Lance commits.
/// Only unordered writes are supported; dataset scan order is not guaranteed.
#[derive(Debug, Clone, Default)]
pub struct LanceDatasetSink {
    storage: Option<Storage>,
    sources: Arc<StorageResolver>,
}

impl LanceDatasetSink {
    pub fn new(storage: Storage) -> Self {
        Self {
            storage: Some(storage),
            sources: Arc::default(),
        }
    }
    /// Share source namespaces, credentials, and the URL-derived cache with a download pool.
    /// This replaces any previously configured source stores.
    pub fn with_source_resolver(mut self, sources: Arc<StorageResolver>) -> Self {
        self.sources = sources;
        self
    }

    /// Configure an independent source namespace for native external blob ingestion.
    /// If the resolver is shared, detach its registrations before adding this store.
    pub fn with_source_storage(mut self, storage: Storage) -> Self {
        self.sources = Arc::new(self.sources.as_ref().clone().with_storage(storage));
        self
    }
    fn resolve(&self, url: Url, options: &WriteOptions) -> Result<LanceDestination> {
        let storage = match &self.storage {
            Some(storage) => storage.clone(),
            None => Storage::from_url(&url)?,
        }
        .with_write_options(options);
        LanceDestination::new(
            storage,
            url,
            self.sources.clone(),
            options.io_metrics.as_ref(),
        )
    }
    /// Configure native Lance reads with the same shared operator as writes.
    pub fn dataset_builder(&self, url: Url) -> Result<lance::dataset::builder::DatasetBuilder> {
        Ok(self
            .resolve(url, &WriteOptions::default())?
            .dataset_builder())
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
                    .with_global(&[
                        CompressionCodec::Uncompressed,
                        CompressionCodec::Zstd,
                        CompressionCodec::Lz4Raw,
                    ])
                    .with_per_column(&[
                        CompressionCodec::Uncompressed,
                        CompressionCodec::Zstd,
                        CompressionCodec::Lz4Raw,
                    ])
                    .with_structural_encodings(&[
                        open_arrow_sink_core::StructuralEncoding::Miniblock,
                        open_arrow_sink_core::StructuralEncoding::Fullzip,
                    ])
                    .with_nested_columns(true),
            )
            .with_supported_partitions(vec![])
            .with_supports_blob_handle(true)
            .with_supports_blob_streaming(true)
            .with_supports_upload_options(true)
            .with_supports_storage_version(true)
            .with_supported_indexes(&[
                IndexType::Scalar,
                IndexType::Vector,
                IndexType::Text,
                IndexType::BloomFilter,
            ])
    }

    async fn open(
        &self,
        url: Url,
        schema: SchemaRef,
        options: WriteOptions,
    ) -> Result<Box<dyn Writer>> {
        if crate::blob::requires_streaming_writer(&schema) {
            let storage_version = validate_options(&schema, &options, self.capability())?;
            let encoding_schema = write_options::writer_schema(&schema, &options)?;
            let params = write_options::write_params(&options, storage_version);
            let mut destination = self.resolve(url, &options)?;
            destination
                .prepare(
                    &crate::blob::logical_schema(&encoding_schema),
                    options.mode,
                    storage_version,
                )
                .await?;
            destination.validate_index_requests(&options).await?;
            return Ok(Box::new(crate::blob_writer::BlobDatasetWriter::new(
                schema,
                encoding_schema,
                destination,
                options,
                params,
            )));
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
    type Output = LanceFragments;
    async fn open_writers(
        &self,
        url: Url,
        schema: SchemaRef,
        options: WriteOptions,
        count: NonZeroUsize,
    ) -> Result<WriteSession<Self::Output>> {
        let storage_version = validate_options(&schema, &options, self.capability())?;
        crate::validation::validate_parallel_blob_schema(&schema)?;
        let encoding_schema = write_options::writer_schema(&schema, &options)?;
        let params = write_options::write_params(&options, storage_version);
        let mut destination = self.resolve(url, &options)?;
        destination
            .prepare(&encoding_schema, options.mode, storage_version)
            .await?;
        destination.validate_index_requests(&options).await?;
        let writers = (0..count.get())
            .map(|_| {
                Box::new(crate::parallel::FragmentWriter::new(
                    schema.clone(),
                    encoding_schema.clone(),
                    destination.clone(),
                    params.clone(),
                )) as Box<dyn DataWriter<Output = Self::Output>>
            })
            .collect();
        Ok(WriteSession {
            writers,
            committer: Box::new(crate::committer::LanceCommitter {
                destination,
                options,
                storage_version,
            }),
        })
    }
}
