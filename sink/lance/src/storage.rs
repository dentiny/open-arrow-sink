use lance::{
    dataset::{CommitBuilder, WriteParams, builder::DatasetBuilder},
    session::Session,
};
use lance_io::object_store::{ObjectStore, ObjectStoreParams, providers::ObjectStoreProvider};
use lance_table::io::commit::ConditionalPutCommitHandler;
use object_store::path::Path;
use object_store_opendal::OpendalStore;
use open_arrow_sink_core::{Error, Result, async_trait};
use open_arrow_sink_storage::{Storage, Url};
use std::sync::Arc;

#[derive(Clone)]
pub(crate) struct NativeStorage {
    storage: Storage,
    pub url: Url,
    session: Arc<Session>,
}
impl NativeStorage {
    pub fn new(storage: Storage, mut url: Url) -> Result<Self> {
        if storage.path(&url)?.trim_matches('/').is_empty() {
            return Err(Error::Unsupported(
                "dataset must have a nonempty path".into(),
            ));
        }
        if !url.path().ends_with('/') {
            url.path_segments_mut().unwrap().push("");
        }
        let session = Arc::new(Session::default());
        session
            .store_registry()
            .insert(url.scheme(), Arc::new(Provider(storage.clone())));
        Ok(Self {
            storage,
            url,
            session,
        })
    }
    pub async fn reserve(&self) -> Result<()> {
        let mut exact = self.url.clone();
        exact.path_segments_mut().unwrap().pop_if_empty();
        if self.storage.exists(&exact).await?
            || self.storage.exists(&self.url).await?
            || !self.storage.list(&self.url).await?.is_empty()
        {
            return Err(crate::error::backend(std::io::Error::new(
                std::io::ErrorKind::AlreadyExists,
                "destination exists",
            )));
        }
        self.storage
            .create(&self.url.join(".open-arrow-sink").unwrap(), "".into())
            .await
    }
    pub async fn cleanup(&self) -> Result<()> {
        self.storage.remove_all(&self.url).await
    }
    pub fn write_params(&self, size: Option<usize>) -> WriteParams {
        WriteParams {
            session: Some(self.session.clone()),
            commit_handler: Some(Arc::new(ConditionalPutCommitHandler)),
            max_bytes_per_file: size.unwrap_or(WriteParams::default().max_bytes_per_file),
            ..Default::default()
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

#[derive(Debug)]
struct Provider(Storage);
fn native(error: impl std::fmt::Display) -> lance::Error {
    lance::Error::invalid_input(error.to_string())
}
#[async_trait]
impl ObjectStoreProvider for Provider {
    async fn new_store(
        &self,
        base_path: Url,
        params: &ObjectStoreParams,
    ) -> lance::Result<ObjectStore> {
        self.0.path(&base_path).map_err(native)?;
        Ok(ObjectStore::new(
            Arc::new(OpendalStore::new(self.0.operator().clone())),
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
        Path::parse(self.0.path(url).map_err(native)?).map_err(native)
    }
    fn calculate_object_store_prefix(
        &self,
        _: &Url,
        _: Option<&std::collections::HashMap<String, String>>,
    ) -> lance::Result<String> {
        Ok(self.0.base_url().to_string())
    }
}
