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
    /// Blob download policy and recursive input materialization.
    blobs: crate::blob_input::BlobMaterializer,
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
        if crate::blob_input::has_blobs(&self.schemas.logical) {
            self.blobs
                .send_rows(&batch, &self.schemas.logical, &mut self.worker)
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
