use crate::provider::Provider;
use crate::storage_router::StorageRouter;
use lance::{
    dataset::{CommitBuilder, WriteParams, builder::DatasetBuilder},
    session::Session,
};
use lance_table::io::commit::ConditionalPutCommitHandler;
use open_arrow_sink_core::{Error, Result, SchemaRef, WriteMode};
use open_arrow_sink_storage::{ObjectPath, Storage, StorageResolver, Url};
use std::sync::Arc;

#[derive(Clone)]
pub(crate) struct NativeStorage {
    pub storage: Storage,
    pub url: Url,
    pub path: ObjectPath,
    session: Arc<Session>,
    pub io_tracker: Arc<crate::io_metrics::TrackedIo>,
    new_destination: bool,
}
impl NativeStorage {
    pub fn new(
        storage: Storage,
        url: Url,
        sources: Arc<StorageResolver>,
        metrics: Option<&open_arrow_sink_core::IoMetrics>,
    ) -> Result<Self> {
        let path = storage.resolve(&url)?.as_dir();
        if path.is_root() {
            return Err(crate::error::root_destination());
        }
        // Lance's native API requires a URL at its boundary.
        let url = storage.url(path.as_str())?;
        let session = Arc::new(Session::default());
        let io_tracker = crate::io_metrics::TrackedIo::new(metrics);
        let provider = Arc::new(Provider {
            storage: StorageRouter::new(storage.clone(), sources),
            io_tracker: io_tracker.clone(),
        });
        for scheme in provider.storage.schemes() {
            session.store_registry().insert(&scheme, provider.clone());
        }
        Ok(Self {
            storage,
            url,
            path,
            session,
            io_tracker,
            new_destination: false,
        })
    }
    pub async fn prepare(
        &mut self,
        schema: &SchemaRef,
        mode: WriteMode,
        storage_version: Option<&str>,
    ) -> Result<()> {
        if mode == WriteMode::Append {
            return crate::validation::validate_existing_dataset(
                self.dataset_builder(),
                Some(schema),
                storage_version,
            )
            .await;
        }
        if self.storage.prefix_exists(&self.path).await? {
            return match mode {
                WriteMode::Create => Err(Error::DestinationExists),
                _ => {
                    crate::validation::validate_existing_dataset(self.dataset_builder(), None, None)
                        .await
                }
            };
        }
        self.new_destination = true;
        Ok(())
    }
    pub async fn validate_index_requests(
        &self,
        options: &open_arrow_sink_core::WriteOptions,
    ) -> Result<()> {
        if options.mode == WriteMode::Append && !options.indexes.is_empty() {
            let dataset = self
                .dataset_builder()
                .load()
                .await
                .map_err(Error::backend)?;
            crate::index::validate_existing_indexes(&dataset, &options.indexes).await?;
        }
        Ok(())
    }
    pub async fn cleanup(&self) -> Result<()> {
        if self.new_destination {
            self.storage.remove_all(&self.path).await?;
        }
        Ok(())
    }
    /// Bind native parameters to this destination's storage and commit policy.
    pub fn bind_write_params(&self, params: WriteParams) -> WriteParams {
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
        let source = sources.resolve(&source_url).unwrap();
        source
            .write(
                &source.resolve(&source_url).unwrap(),
                "custom backend".into(),
            )
            .await
            .unwrap();
        let destination = Storage::new(
            opendal::Operator::new(opendal::services::Memory::default()).unwrap(),
            "destination-only://warehouse/".parse().unwrap(),
        )
        .unwrap();
        let url = destination.url("dataset/").unwrap();
        let native = NativeStorage::new(destination, url, sources, None).unwrap();
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
