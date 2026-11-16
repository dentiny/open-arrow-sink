use crate::error::native_storage as native;
use lance_io::object_store::{ObjectStore, ObjectStoreParams, providers::ObjectStoreProvider};
use object_store::path::Path;
use object_store_opendal::OpendalStore;
use open_arrow_sink_core::{Result, async_trait};
use open_arrow_sink_storage::{Storage, StorageResolver, Url};
use std::sync::Arc;

/// Adapt the destination and independently resolved sources to Lance.
#[derive(Debug)]
pub(crate) struct Provider {
    destination: Storage,
    sources: StorageResolver,
    io_tracker: Arc<crate::io_metrics::TrackedIo>,
}
impl Provider {
    pub fn new(
        destination: Storage,
        sources: StorageResolver,
        io_tracker: Arc<crate::io_metrics::TrackedIo>,
    ) -> Self {
        Self {
            destination,
            sources,
            io_tracker,
        }
    }

    /// Adapt storage capabilities to Lance's per-scheme provider registry.
    pub fn schemes(&self) -> impl Iterator<Item = String> {
        self.sources.schemes().chain(std::iter::once(
            self.destination.base_url().scheme().to_owned(),
        ))
    }

    /// The destination owns its namespace, even if a more specific source is registered.
    pub fn resolve(&self, url: &Url) -> Result<Storage> {
        if self.destination.path(url).is_ok() {
            Ok(self.destination.clone())
        } else {
            self.sources.resolve(url)
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
        let storage = self.resolve(&base_path).map_err(native)?;
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
            self.resolve(url)
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
        Ok(self.resolve(url).map_err(native)?.base_url().to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use open_arrow_sink_storage::opendal;

    fn memory(base: &str) -> Storage {
        Storage::new(
            opendal::Operator::new(opendal::services::Memory::default()).unwrap(),
            base.parse().unwrap(),
        )
        .unwrap()
    }

    #[tokio::test]
    async fn destination_owns_its_namespace_even_with_a_more_specific_source() {
        let destination = memory("s3://destination/");
        let overlap = memory("s3://destination/nested/");
        let external = memory("s3://source/");
        let local_url = destination.url("nested/key").unwrap();
        let external_url = external.url("key").unwrap();
        destination
            .write(&local_url, "destination".into())
            .await
            .unwrap();
        overlap.write(&local_url, "overlap".into()).await.unwrap();
        external
            .write(&external_url, "source".into())
            .await
            .unwrap();

        let provider = Provider::new(
            destination,
            StorageResolver::default()
                .with_storage(overlap)
                .with_storage(external),
            crate::io_metrics::TrackedIo::new(None),
        );
        assert_eq!(
            provider
                .resolve(&local_url)
                .unwrap()
                .read(&local_url)
                .await
                .unwrap(),
            "destination"
        );
        assert_eq!(
            provider
                .resolve(&external_url)
                .unwrap()
                .read(&external_url)
                .await
                .unwrap(),
            "source"
        );
    }
}
