use open_arrow_sink_storage::{BlobDownloader, Storage, StorageResolver, Url};
use std::num::NonZeroUsize;
use std::sync::Arc;

use open_arrow_sink_core::{
    CompressionCapability, CompressionCodec, DataWriter, IndexType, Ordering, ParallelSink, Result,
    SchemaRef, Sink, SinkCapability, WriteMode, WriteOptions, WriteSession, Writer, async_trait,
};

use crate::{storage::NativeStorage, validation::validate_options, write_options};

/// Creates, appends to, or version-overwrites a URL dataset using native Lance commits.
/// Only unordered writes are supported; dataset scan order is not guaranteed.
#[derive(Debug, Clone)]
pub struct LanceDatasetSink {
    storage: Option<Storage>,
    sources: StorageResolver,
    encoding_buffer_size: Option<NonZeroUsize>,
    downloader: Arc<BlobDownloader>,
}

impl Default for LanceDatasetSink {
    fn default() -> Self {
        Self {
            storage: None,
            sources: StorageResolver::default(),
            encoding_buffer_size: None,
            downloader: Arc::new(
                BlobDownloader::with_defaults().expect("valid default download limits"),
            ),
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
    /// Replace the always-present blob downloader with shared, configured limits.
    pub fn with_blob_downloader(mut self, downloader: Arc<BlobDownloader>) -> Self {
        self.downloader = downloader;
        self
    }
    /// Set Lance's total column-data cache per physical writer. Divide an
    /// application's encoding budget by its writer count when using parallel
    /// writers. Transport/upload and downloaded payload budgets are separate.
    pub fn with_encoding_buffer_size(mut self, bytes: NonZeroUsize) -> Self {
        self.encoding_buffer_size = Some(bytes);
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
        let mut destination =
            NativeStorage::new(storage.clone(), url, self.sources.clone(), metrics)?;
        destination.encoding_buffer_size = self.encoding_buffer_size;
        destination.downloader = Arc::new(
            self.downloader
                .as_ref()
                .clone()
                .with_source_resolver(&self.sources)
                .with_source_storage(storage),
        );
        Ok(destination)
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
            .with_supported_partitions(vec![])
            .with_supports_blob_handle(true)
            .with_supports_blob_streaming(true)
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
        let storage_schema = crate::blob::storage_schema(&schema)?;
        if storage_schema != schema {
            validate_options(&schema, &options, self.capability())?;
            let encoding_schema = write_options::writer_schema(&schema, &options)?;
            let mut destination = self.resolve(url, options.io_metrics.as_ref())?;
            destination
                .prepare(
                    &crate::blob::logical_schema(&encoding_schema),
                    options.mode,
                    options.storage_version.as_deref(),
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
        validate_options(&schema, &options, self.capability())?;
        crate::validation::validate_blob_schema(&schema)?;
        let encoding_schema =
            crate::blob::logical_schema(&write_options::writer_schema(&schema, &options)?);
        let mut destination = self.resolve(url, options.io_metrics.as_ref())?;
        destination
            .prepare(
                &encoding_schema,
                options.mode,
                options.storage_version.as_deref(),
            )
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
