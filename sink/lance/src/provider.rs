use crate::error::native_storage as native;
use lance_io::object_store::{ObjectStore, ObjectStoreParams, providers::ObjectStoreProvider};
use object_store::path::Path;
use open_arrow_sink_core::{Result, async_trait};
use open_arrow_sink_storage::{Storage, StorageResolver, Url};
use std::sync::Arc;

/// Adapt the destination and independently resolved sources to Lance.
#[derive(Debug)]
pub(crate) struct Provider {
    destination: Storage,
    sources: Arc<StorageResolver>,
    metrics: Option<open_arrow_sink_core::IoObservers>,
}
impl Provider {
    pub fn new(
        destination: Storage,
        sources: Arc<StorageResolver>,
        metrics: Option<open_arrow_sink_core::IoObservers>,
    ) -> Self {
        Self {
            destination,
            sources,
            metrics,
        }
    }

    /// Adapt storage capabilities to Lance's per-scheme provider registry.
    pub fn schemes(&self) -> impl Iterator<Item = String> {
        self.sources.schemes().chain(std::iter::once(
            self.destination.base_url().scheme().to_owned(),
        ))
    }

    /// The destination owns its namespace, even with a more specific source.
    pub fn storage_for(&self, url: &Url) -> Result<Storage> {
        if self.destination.path(url).is_ok() {
            Ok(self.destination.clone())
        } else {
            self.sources
                .storage_for(url)
                .map(|storage| match &self.metrics {
                    Some(metrics) => storage.with_io_metrics(metrics.clone()),
                    None => storage,
                })
        }
    }
}

#[async_trait]
impl ObjectStoreProvider for Provider {
    async fn new_store(
        &self,
        base_path: Url,
        params: &ObjectStoreParams,
    ) -> lance::Result<ObjectStore> {
        let storage = self.storage_for(&base_path).map_err(native)?;
        let object_store = storage.object_store();
        let mut store = ObjectStore::new(
            object_store,
            base_path,
            params.block_size,
            params.object_store_wrapper.clone(),
            params.use_constant_size_upload_parts,
            false,
            lance_io::object_store::DEFAULT_CLOUD_IO_PARALLELISM,
            // These retries run above the adapter and can count a fresh read.
            // Storage excludes only retries inside its OpenDAL retry layer.
            lance_io::object_store::DEFAULT_DOWNLOAD_RETRY_COUNT,
            None,
        );
        // Prevent Lance's direct-file shortcut from bypassing the configured operator.
        store.store_prefix = storage.base_url().to_string();
        Ok(store)
    }
    fn extract_path(&self, url: &Url) -> lance::Result<Path> {
        // OpendalStore decodes object-store keys. Encode literal percent signs
        // as PathParts so a URL containing %2520 stays a literal "%20" key.
        Ok(Path::from(
            self.storage_for(url)
                .map_err(native)?
                .path(url)
                .map_err(native)?
                .as_str(),
        ))
    }
    fn calculate_object_store_prefix(
        &self,
        url: &Url,
        _: Option<&std::collections::HashMap<String, String>>,
    ) -> lance::Result<String> {
        Ok(self
            .storage_for(url)
            .map_err(native)?
            .base_url()
            .to_string())
    }
}
