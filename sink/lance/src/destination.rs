use crate::provider::Provider;
use lance::{
    dataset::{CommitBuilder, WriteParams, builder::DatasetBuilder},
    session::Session,
};
use lance_table::io::commit::ConditionalPutCommitHandler;
use open_arrow_sink_core::{Error, Result, SchemaRef, WriteMode};
use open_arrow_sink_storage::{DirectoryState, ObjectPath, Storage, StorageResolver, Url};
use std::sync::Arc;

/// Dataset location, native session, and storage configuration.
#[derive(Clone)]
pub(crate) struct LanceDestination {
    pub storage: Storage,
    pub downloader: Arc<open_arrow_sink_storage::BlobDownloader>,
    pub url: Url,
    pub path: ObjectPath,
    session: Arc<Session>,
    pub encoding_buffer_size: Option<std::num::NonZeroUsize>,
}
impl LanceDestination {
    pub fn new(
        storage: Storage,
        url: Url,
        sources: Arc<StorageResolver>,
        downloader: Arc<open_arrow_sink_storage::BlobDownloader>,
        metrics: Option<&open_arrow_sink_core::IoObservers>,
    ) -> Result<Self> {
        let path = storage.path(&url)?.as_dir();
        if path.is_root() {
            return Err(crate::error::invalid_destination_root());
        }
        // Lance's native API requires a URL at its boundary.
        let url = storage.url(&path)?;
        let session = Arc::new(Session::default());
        let provider = Arc::new(Provider::new(storage.clone(), sources, metrics.cloned()));
        for scheme in provider.schemes() {
            session.store_registry().insert(&scheme, provider.clone());
        }
        Ok(Self {
            storage,
            downloader,
            url,
            path,
            session,
            encoding_buffer_size: None,
        })
    }
    /// Validate the destination and return an existing dataset for subsequent checks.
    pub async fn prepare(
        &self,
        schema: &SchemaRef,
        mode: WriteMode,
        storage_version: Option<lance_file::version::LanceFileVersion>,
    ) -> Result<Option<lance::Dataset>> {
        if mode == WriteMode::Append {
            return crate::validation::validate_existing_dataset(
                self.dataset_builder(),
                Some(schema),
                storage_version,
            )
            .await
            .map(Some);
        }
        if self.storage.inspect_directory(&self.path).await? == DirectoryState::Missing {
            return Ok(None);
        }
        if mode == WriteMode::Create {
            return Err(Error::AlreadyExists(self.url.to_string()));
        }
        crate::validation::validate_existing_dataset(self.dataset_builder(), None, None)
            .await
            .map(Some)
    }
    /// Bind native parameters to this destination's storage and commit policy.
    pub fn bind_write_params(&self, mut params: WriteParams) -> WriteParams {
        if let Some(bytes) = self.encoding_buffer_size {
            params
                .file_writer_options
                .get_or_insert_with(Default::default)
                .data_cache_bytes = Some(bytes.get() as u64);
        }
        WriteParams {
            session: Some(self.session.clone()),
            commit_handler: Some(Arc::new(ConditionalPutCommitHandler)),
            ..params
        }
    }
    pub fn commit_builder(&self) -> CommitBuilder<'_> {
        CommitBuilder::new(self.url.as_str())
            .with_session(self.session.clone())
            .with_commit_handler(Arc::new(ConditionalPutCommitHandler))
    }
    /// Candidate manifests can share a version with another writer or the
    /// published manifest. Keep their metadata caches separate from readers.
    pub fn isolated_session(&self) -> Arc<Session> {
        Arc::new(Session::new(
            lance::dataset::DEFAULT_INDEX_CACHE_SIZE,
            lance::dataset::DEFAULT_METADATA_CACHE_SIZE,
            self.session.store_registry(),
        ))
    }
    pub fn dataset_builder(&self) -> DatasetBuilder {
        DatasetBuilder::from_uri(self.url.as_str())
            .with_session(self.session.clone())
            .with_commit_handler(Arc::new(ConditionalPutCommitHandler))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use lance_io::object_store::ObjectStoreParams;
    use object_store::ObjectStoreExt;
    use open_arrow_sink_storage::opendal;

    #[tokio::test]
    async fn session_routes_opendal_registered_service_without_explicit_source() {
        opendal::OperatorRegistry::get().register::<opendal::services::Memory>("lance-custom");
        let sources = Arc::new(StorageResolver::default());
        let source_url: Url = "lance-custom://source/blob".parse().unwrap();
        // Seed the URL-derived cache; no source namespace is explicitly registered.
        let source = sources.storage_for(&source_url).unwrap();
        source
            .write(&source.path(&source_url).unwrap(), "custom backend".into())
            .await
            .unwrap();
        let destination = Storage::new(
            opendal::Operator::new(opendal::services::Memory::default()).unwrap(),
            "destination-only://warehouse/".parse().unwrap(),
        )
        .unwrap();
        let url = destination
            .url(&ObjectPath::parse("dataset/").unwrap())
            .unwrap();
        let downloader =
            Arc::new(open_arrow_sink_storage::BlobDownloader::with_defaults().unwrap());
        let native = LanceDestination::new(destination, url, sources, downloader, None).unwrap();
        let registry = native.session.store_registry();
        assert!(registry.get_provider("destination-only").is_some());
        let provider = registry.get_provider(source_url.scheme()).unwrap();
        let path = provider.extract_path(&source_url).unwrap();
        let store = registry
            .get_store(source_url, &ObjectStoreParams::default())
            .await
            .unwrap();
        assert_eq!(
            store.inner.get(&path).await.unwrap().bytes().await.unwrap(),
            "custom backend"
        );
    }
}
