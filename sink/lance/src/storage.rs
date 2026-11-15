use crate::error::native_storage as native;
use lance::{
    dataset::{CommitBuilder, WriteParams, builder::DatasetBuilder},
    session::Session,
};
use lance_io::object_store::{ObjectStore, ObjectStoreParams, providers::ObjectStoreProvider};
use lance_table::io::commit::ConditionalPutCommitHandler;
use object_store::path::Path;
use object_store_opendal::OpendalStore;
use open_arrow_sink_core::{Result, SchemaRef, WriteMode, WriteOptions, async_trait};
use open_arrow_sink_storage::{Storage, Url};
use std::sync::Arc;

#[derive(Clone)]
pub(crate) struct NativeStorage {
    storage: Storage,
    pub url: Url,
    session: Arc<Session>,
    reserved: bool,
}
impl NativeStorage {
    pub fn new(storage: Storage, url: Url) -> Result<Self> {
        let url = storage.directory_url(&url)?;
        let session = Arc::new(Session::default());
        session
            .store_registry()
            .insert(url.scheme(), Arc::new(Provider(storage.clone())));
        Ok(Self {
            storage,
            url,
            session,
            reserved: false,
        })
    }
    pub async fn prepare(&mut self, schema: &SchemaRef, mode: WriteMode) -> Result<()> {
        match mode {
            WriteMode::Create => self.reserve().await?,
            WriteMode::Append => {
                crate::validation::validate_existing_dataset(self.dataset_builder(), Some(schema))
                    .await?
            }
            WriteMode::Overwrite => {
                if self.storage.prefix_exists(&self.url).await? {
                    crate::validation::validate_existing_dataset(self.dataset_builder(), None)
                        .await?;
                } else {
                    self.reserve().await?;
                }
            }
        }
        Ok(())
    }
    async fn reserve(&mut self) -> Result<()> {
        if self.storage.prefix_exists(&self.url).await? {
            return Err(crate::error::destination_exists());
        }
        self.storage
            .create(&self.url.join(".open-arrow-sink").unwrap(), "".into())
            .await?;
        self.reserved = true;
        Ok(())
    }
    pub async fn cleanup(&self) -> Result<()> {
        if self.reserved {
            self.storage.remove_all(&self.url).await?;
        }
        Ok(())
    }
    pub fn write_params(&self, options: &WriteOptions) -> WriteParams {
        WriteParams {
            session: Some(self.session.clone()),
            commit_handler: Some(Arc::new(ConditionalPutCommitHandler)),
            mode: match options.mode {
                WriteMode::Create => lance::dataset::WriteMode::Create,
                WriteMode::Append => lance::dataset::WriteMode::Append,
                WriteMode::Overwrite => lance::dataset::WriteMode::Overwrite,
            },
            max_bytes_per_file: options
                .target_file_size
                .unwrap_or(WriteParams::default().max_bytes_per_file),
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
