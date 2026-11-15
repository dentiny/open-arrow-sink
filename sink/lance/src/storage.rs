use crate::provider::Provider;
use lance::{
    dataset::{CommitBuilder, WriteParams, builder::DatasetBuilder},
    session::Session,
};
use lance_table::io::commit::ConditionalPutCommitHandler;
use open_arrow_sink_core::{Result, SchemaRef, WriteMode, WriteOptions};
use open_arrow_sink_storage::{Storage, Url};
use std::sync::Arc;

#[derive(Clone)]
pub(crate) struct NativeStorage {
    pub storage: Storage,
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
        if mode == WriteMode::Append {
            return crate::validation::validate_existing_dataset(
                self.dataset_builder(),
                Some(schema),
            )
            .await;
        }
        if self.storage.prefix_exists(&self.url).await? {
            return match mode {
                WriteMode::Create => Err(crate::error::destination_exists()),
                _ => {
                    crate::validation::validate_existing_dataset(self.dataset_builder(), None).await
                }
            };
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
    pub fn write_params(&self, options: &WriteOptions) -> Result<WriteParams> {
        let blob = options.blob_handle.clone().unwrap_or_default();
        Ok(WriteParams {
            external_blob_mode: match blob.external_blob_mode {
                open_arrow_sink_core::ExternalBlobMode::Reference => {
                    lance::dataset::ExternalBlobMode::Reference
                }
                open_arrow_sink_core::ExternalBlobMode::Ingest => {
                    lance::dataset::ExternalBlobMode::Ingest
                }
            },
            allow_external_blob_outside_bases: blob.allow_external_blob_outside_bases,
            blob_pack_file_size_threshold: blob.blob_pack_file_size_threshold,
            data_storage_version: crate::validation::parse_storage_version(
                options.storage_version.as_deref(),
            )?,
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
        })
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
