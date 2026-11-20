use lance::dataset::WriteParams;
use open_arrow_sink_core::{
    DataWriter, Poison, RecordBatch, Result, SchemaRef, WriteOptions, async_trait, validation,
};

use crate::{
    LanceFragments, compression::writer_batch, destination::LanceDestination, worker::FragmentTask,
};

/// Writes uncommitted fragments. No worker can publish a dataset manifest.
pub(crate) struct FragmentWriter {
    /// Caller input schema for validation and Lance logical schema for encoding.
    schemas: crate::write_options::LanceSchemas,
    /// Background worker that receives RecordBatches from a bounded channel and
    /// writes them to uncommitted fragment files. No input batches means no
    /// fragment files are created; manifest publication belongs to the committer.
    worker: FragmentTask,
    /// Prevents reuse after an error, cancellation, or finish.
    state: Poison,
    blob_plan: crate::blob_input::BlobInputPlan,
    /// Blob download policy and recursive input materialization.
    blobs: crate::blob_input::BlobMaterializer,
}

impl FragmentWriter {
    pub(crate) fn new(
        schemas: crate::write_options::LanceSchemas,
        destination: &LanceDestination,
        existing: Option<std::sync::Arc<lance::Dataset>>,
        params: WriteParams,
        options: &WriteOptions,
    ) -> Self {
        let worker = FragmentTask::new(schemas.logical.clone(), destination, existing, params);
        let blob_plan = crate::blob_input::BlobInputPlan::new(schemas.logical.clone());
        Self {
            schemas,
            blob_plan,
            worker,
            state: Poison::default(),
            blobs: crate::blob_input::BlobMaterializer::new(
                destination.downloader.clone(),
                options,
            ),
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
        if !self.blob_plan.is_empty() {
            self.blobs
                .send_rows(&batch, &self.blob_plan, &mut self.worker)
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
        let mut output = self.worker.finish().await?;
        let downloads = self.blobs.downloads.snapshot();
        output.summary.rows_skipped = downloads.rows_skipped;
        output.summary.rows_failed_blobs = downloads.rows_failed_blobs;
        Ok(output)
    }
    async fn abort(mut self: Box<Self>) -> Result<()> {
        self.worker.abort().await;
        // Persisted output is retained after stopping the worker.
        Ok(())
    }
}
