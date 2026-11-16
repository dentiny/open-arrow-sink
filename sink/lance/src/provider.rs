use crate::error::native_storage as native;
use crate::storage_router::StorageRouter;
use lance_io::object_store::{ObjectStore, ObjectStoreParams, providers::ObjectStoreProvider};
use object_store::path::Path;
use object_store_opendal::OpendalStore;
use open_arrow_sink_core::async_trait;
use open_arrow_sink_storage::Url;
use std::sync::Arc;

/// Adapt routed storage to Lance; namespace selection belongs to StorageRouter.
#[derive(Debug)]
pub(crate) struct Provider {
    pub storage: StorageRouter,
    pub io_tracker: Arc<crate::io_metrics::TrackedIo>,
}
#[async_trait]
impl ObjectStoreProvider for Provider {
    async fn new_store(
        &self,
        base_path: Url,
        params: &ObjectStoreParams,
    ) -> lance::Result<ObjectStore> {
        let storage = self.storage.resolve(&base_path).map_err(native)?;
        let object_store: Arc<dyn object_store::ObjectStore> = match storage.upload_part_size() {
            Some(size) => Arc::new(crate::upload::UploadStore::new(
                storage.operator(),
                size.get(),
            )),
            None => Arc::new(OpendalStore::new(storage.operator())),
        };
        use lance_io::object_store::WrappingObjectStore;
        let object_store = self
            .io_tracker
            .wrap(storage.base_url().as_str(), object_store);
        let mut store = ObjectStore::new(
            object_store,
            base_path,
            params.block_size,
            params.object_store_wrapper.clone(),
            params.use_constant_size_upload_parts,
            false,
            lance_io::object_store::DEFAULT_CLOUD_IO_PARALLELISM,
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
            self.storage
                .resolve(url)
                .map_err(native)?
                .path(url)
                .map_err(native)?,
        ))
    }
    fn calculate_object_store_prefix(
        &self,
        url: &Url,
        _: Option<&std::collections::HashMap<String, String>>,
    ) -> lance::Result<String> {
        Ok(self
            .storage
            .resolve(url)
            .map_err(native)?
            .base_url()
            .to_string())
    }
}
