use open_arrow_sink_storage::{BlobDownloader, Storage, StorageConfig, StorageResolver, Url};
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
#[derive(Debug, Clone)]
pub struct LanceDatasetSink {
    storage: Option<Storage>,
    // Clones share URL-derived backends and their HTTP request budgets.
    resolved: StorageResolver,
    config: StorageConfig,
    sources: Option<Arc<StorageResolver>>,
    downloader: Arc<BlobDownloader>,
    encoding_buffer_size: Option<NonZeroUsize>,
}

impl Default for LanceDatasetSink {
    fn default() -> Self {
        Self {
            storage: None,
            resolved: StorageResolver::default(),
            config: StorageConfig::default(),
            sources: None,
            downloader: Arc::new(
                BlobDownloader::with_defaults().expect("valid default download limits"),
            ),
            encoding_buffer_size: None,
        }
    }
}
impl LanceDatasetSink {
    pub fn new(storage: Storage) -> Self {
        Self {
            storage: Some(storage),
            ..Self::default()
        }
    }
    /// Configure retry/timeout policies for explicit or URL-derived destination storage.
    /// Validation happens now, before opening a destination or performing I/O.
    /// External source stores retain their independently configured policies.
    pub fn with_config(mut self, config: StorageConfig) -> Result<Self> {
        config.validate()?;
        self.config = config;
        Ok(self)
    }

    pub fn with_blob_downloader(mut self, downloader: Arc<BlobDownloader>) -> Self {
        self.downloader = downloader;
        self
    }
    /// Native column-data cache per physical writer, separate from download/upload buffers.
    pub fn with_encoding_buffer_size(mut self, bytes: NonZeroUsize) -> Self {
        self.encoding_buffer_size = Some(bytes);
        self
    }
    /// Share source namespaces, credentials, and the URL-derived cache with a download pool.
    /// This replaces any previously configured source stores.
    pub fn with_source_resolver(mut self, sources: Arc<StorageResolver>) -> Self {
        self.sources = Some(sources);
        self
    }

    /// Configure an independent source namespace for native external blob ingestion.
    /// If the resolver is shared, detach its registrations before adding this store.
    pub fn with_source_storage(mut self, storage: Storage) -> Self {
        self.sources = Some(Arc::new(
            self.source_resolver()
                .as_ref()
                .clone()
                .with_storage(storage),
        ));
        self
    }
    fn source_resolver(&self) -> Arc<StorageResolver> {
        self.sources
            .clone()
            .unwrap_or_else(|| self.downloader.source_resolver())
    }
    fn destination_for(&self, url: Url, options: &WriteOptions) -> Result<LanceDestination> {
        let sources = self.source_resolver();
        let storage = match &self.storage {
            Some(storage) => storage.clone(),
            None => self.resolved.storage_for(&url)?,
        }
        .with_config(self.config.clone())?;
        let downloader = Arc::new(
            self.downloader
                .as_ref()
                .clone()
                .with_source_resolver(sources.clone())
                .with_primary_storage(storage.clone()),
        );
        let mut destination = LanceDestination::new(
            storage.with_write_options(options),
            url,
            sources,
            downloader,
            options.io_metrics.as_ref(),
        )?;
        destination.encoding_buffer_size = self.encoding_buffer_size;
        Ok(destination)
    }
    /// Configure native Lance reads with the same shared operator as writes.
    pub fn dataset_builder(&self, url: Url) -> Result<lance::dataset::builder::DatasetBuilder> {
        Ok(self
            .destination_for(url, &WriteOptions::default())?
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
        // Use the same fragment writing and publication path for every session.
        self.open_writer(url, schema, options).await
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
        let schemas = write_options::LanceSchemas::new(schema, &options)?;
        let params = write_options::write_params(&options, storage_version);
        let destination = self.destination_for(url, &options)?;
        let existing = destination
            .prepare(&schemas.logical, options.mode, storage_version)
            .await?;
        crate::validation::validate_index_requests(existing.as_ref(), &options).await?;
        let writers = (0..count.get())
            .map(|_| {
                Box::new(crate::parallel::FragmentWriter::new(
                    schemas.clone(),
                    &destination,
                    params.clone(),
                    &options,
                )) as Box<dyn DataWriter<Output = Self::Output>>
            })
            .collect();
        Ok(WriteSession {
            writers,
            committer: Box::new(crate::committer::LanceCommitter {
                destination,
                indexes: options.indexes,
                storage_version,
            }),
        })
    }
}
