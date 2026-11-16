use crate::provider::Provider;
use lance::{
    dataset::{CommitBuilder, WriteParams, builder::DatasetBuilder},
    session::Session,
};
use lance_table::io::commit::ConditionalPutCommitHandler;
use open_arrow_sink_core::{Result, SchemaRef, WriteMode};
use open_arrow_sink_storage::{ObjectPath, Storage, StorageResolver, Url};
use std::sync::Arc;

/// Dataset location, native session, and owned publication reservation.
#[derive(Clone)]
pub(crate) struct LanceDestination {
    pub storage: Storage,
    pub url: Url,
    pub path: ObjectPath,
    session: Arc<Session>,
    new_destination: bool,
}
impl LanceDestination {
    pub fn new(
        storage: Storage,
        url: Url,
        sources: Arc<StorageResolver>,
        metrics: Option<&open_arrow_sink_core::IoMetrics>,
    ) -> Result<Self> {
        let storage = match metrics {
            Some(metrics) => storage.with_io_metrics(metrics.clone()),
            None => storage,
        };
        let path = storage.resolve(&url)?.as_dir();
        crate::validation::validate_destination_path(&path)?;
        // Lance's native API requires a URL at its boundary.
        let url = storage.url(path.as_str())?;
        let session = Arc::new(Session::default());
        let provider = Arc::new(Provider::new(storage.clone(), sources, metrics.cloned()));
        for scheme in provider.schemes() {
            session.store_registry().insert(&scheme, provider.clone());
        }
        Ok(Self {
            storage,
            url,
            path,
            session,
            new_destination: false,
        })
    }
    pub async fn prepare(
        &mut self,
        schema: &SchemaRef,
        mode: WriteMode,
        storage_version: Option<lance_file::version::LanceFileVersion>,
    ) -> Result<Option<lance::Dataset>> {
        let existing =
            crate::validation::validate_destination(self, schema, mode, storage_version).await?;
        self.new_destination = existing.is_none();
        Ok(existing)
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
        let native = LanceDestination::new(destination, url, sources, None).unwrap();
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
