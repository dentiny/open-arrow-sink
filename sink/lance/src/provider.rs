use crate::error::native_storage as native;
use lance_io::object_store::{ObjectStore, ObjectStoreParams, providers::ObjectStoreProvider};
use object_store::path::Path;
use object_store_opendal::OpendalStore;
use open_arrow_sink_core::async_trait;
use open_arrow_sink_storage::{Storage, Url};
use std::sync::Arc;

#[derive(Debug)]
pub(crate) struct Provider(pub(crate) Storage);
#[async_trait]
impl ObjectStoreProvider for Provider {
    async fn new_store(
        &self,
        base_path: Url,
        params: &ObjectStoreParams,
    ) -> lance::Result<ObjectStore> {
        self.0.path(&base_path).map_err(native)?;
        Ok(ObjectStore::new(
            Arc::new(OpendalStore::new(self.0.operator())),
            base_path,
            params.block_size,
            params.object_store_wrapper.clone(),
            params.use_constant_size_upload_parts,
            false,
            lance_io::object_store::DEFAULT_CLOUD_IO_PARALLELISM,
            lance_io::object_store::DEFAULT_DOWNLOAD_RETRY_COUNT,
            None,
        ))
    }
    fn extract_path(&self, url: &Url) -> lance::Result<Path> {
        // OpendalStore decodes object-store keys. Encode literal percent signs
        // as PathParts so a URL containing %2520 stays a literal "%20" key.
        Ok(Path::from(self.0.path(url).map_err(native)?))
    }
    fn calculate_object_store_prefix(
        &self,
        _: &Url,
        _: Option<&std::collections::HashMap<String, String>>,
    ) -> lance::Result<String> {
        Ok(self.0.base_url().to_string())
    }
}
