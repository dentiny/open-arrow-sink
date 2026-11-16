use lance::dataset::WriteParams;
use open_arrow_sink_core::{
    DataWriter, Poison, RecordBatch, Result, SchemaRef, WriteOptions, async_trait, validation,
};

use crate::{
    LanceFragments, compression::writer_batch, destination::LanceDestination, worker::FragmentTask,
};

/// Writes uncommitted fragments. No worker can publish a dataset manifest.
pub(crate) struct FragmentWriter {
    schemas: crate::write_options::LanceSchemas,
    worker: FragmentTask,
    state: Poison,
    downloader: std::sync::Arc<open_arrow_sink_storage::BlobDownloader>,
    blob_mode: open_arrow_sink_core::ExternalBlobMode,
    recovery: Option<open_arrow_sink_core::DownloadRecovery>,
    io_metrics: Option<open_arrow_sink_core::IoMetrics>,
}

impl FragmentWriter {
    pub(crate) fn new(
        schemas: crate::write_options::LanceSchemas,
        destination: &LanceDestination,
        params: WriteParams,
        options: &WriteOptions,
    ) -> Self {
        let worker = FragmentTask::new(schemas.logical.clone(), destination, params);
        Self {
            schemas,
            worker,
            state: Poison::default(),
            downloader: destination.downloader.clone(),
            blob_mode: options
                .blob_handle
                .as_ref()
                .map(|config| config.external_blob_mode)
                .unwrap_or_default(),
            io_metrics: options.io_metrics.clone(),
            recovery: options.download_recovery.clone(),
        }
    }
}

#[async_trait]
impl DataWriter for FragmentWriter {
    type Output = LanceFragments;
    fn schema(&self) -> &SchemaRef {
        &self.schemas.input
    }
    async fn write(&mut self, batch: RecordBatch) -> Result<()> {
        self.state.begin()?;
        validation::validate_batch(&self.schemas.input, &batch)?;
        if crate::blob_input::has_blobs(&self.schemas.logical) {
            crate::blob_input::send_rows(
                &batch,
                &self.schemas.logical,
                &self.downloader,
                self.blob_mode,
                self.io_metrics.as_ref(),
                self.recovery.as_ref(),
                &mut self.worker,
            )
            .await?;
        } else {
            let batch = writer_batch(batch, &self.schemas.logical)?;
            self.worker.write(batch).await?;
        }
        self.state.complete();
        Ok(())
    }
    async fn finish(&mut self) -> Result<Self::Output> {
        self.state.begin()?;
        self.worker.finish().await
    }
    async fn abort(mut self: Box<Self>) -> Result<()> {
        self.worker.abort().await;
        // Persisted output is retained after stopping the worker.
        Ok(())
    }
}
