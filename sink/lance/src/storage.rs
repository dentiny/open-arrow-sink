use crate::provider::Provider;
use lance::{
    dataset::{CommitBuilder, WriteParams, builder::DatasetBuilder},
    session::Session,
};
use lance_table::io::commit::ConditionalPutCommitHandler;
use open_arrow_sink_core::{Result, SchemaRef, WriteMode};
use open_arrow_sink_storage::{Storage, StorageResolver, Url};
use std::sync::Arc;

#[derive(Clone)]
pub(crate) struct NativeStorage {
    pub storage: Storage,
    pub url: Url,
    session: Arc<Session>,
    pub io_tracker: Arc<lance_io::utils::tracking_store::IOTracker>,
    reserved: bool,
}
impl NativeStorage {
    pub fn new(storage: Storage, url: Url, sources: StorageResolver) -> Result<Self> {
        let url = storage.directory_url(&url)?;
        let session = Arc::new(Session::default());
        let io_tracker = Arc::new(lance_io::utils::tracking_store::IOTracker::default());
        let provider = Arc::new(Provider {
            destination: storage.clone(),
            sources: sources.clone(),
            io_tracker: io_tracker.clone(),
        });
        for scheme in ["file", "s3", "http", "https", "gs", "az", "abfs", "abfss"]
            .into_iter()
            .chain(sources.schemes())
            .chain(std::iter::once(url.scheme()))
        {
            session.store_registry().insert(scheme, provider.clone());
        }
        Ok(Self {
            storage,
            url,
            session,
            io_tracker,
            reserved: false,
        })
    }
    pub fn summary(&self, rows_written: u64) -> open_arrow_sink_core::WriteSummary {
        let stats = self.io_tracker.stats();
        open_arrow_sink_core::WriteSummary {
            rows_written,
            bytes_read: Some(stats.read_bytes),
            bytes_written: Some(stats.written_bytes),
        }
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
