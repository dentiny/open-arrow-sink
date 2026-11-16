use std::sync::Arc;

use lance::dataset::WriteParams;
use open_arrow_sink_core::{
    BlobPlacement, BlobWriter, Poison, RecordBatch, Result, SchemaRef, SessionCommitter,
    WriteOptions, WriteSummary, Writer, async_trait, validation,
};

use crate::{committer::LanceCommitter, destination::LanceDestination, worker::FragmentWorker};

pub(crate) struct BlobDatasetWriter {
    schemas: crate::write_options::LanceSchemas,
    blob_session: Arc<crate::blob::BlobSession>,
    worker: FragmentWorker,
    state: Poison,
    destination: LanceDestination,
    options: WriteOptions,
    storage_version: Option<lance_file::version::LanceFileVersion>,
}

#[async_trait]
impl Writer for BlobDatasetWriter {
    fn schema(&self) -> &SchemaRef {
        &self.schemas.input
    }

    async fn write(&mut self, batch: RecordBatch) -> Result<()> {
        self.write_with_blobs(batch, Vec::new()).await
    }

    async fn create_blob(&mut self) -> Result<Box<dyn BlobWriter>> {
        self.state.check()?;
        self.blob_session.check()?;
        self.state.poison();
        let blob = self
            .blob_session
            .create_blob(&self.destination.path)
            .await?;
        self.state.complete();
        Ok(blob)
    }

    async fn write_with_blobs(
        &mut self,
        batch: RecordBatch,
        blobs: Vec<BlobPlacement>,
    ) -> Result<()> {
        self.state.check()?;
        self.blob_session.check()?;
        self.state.poison();
        validation::validate_batch(&self.schemas.input, &batch)?;
        let batch =
            crate::blob::storage_batch(batch, &self.schemas.encoded, blobs, &self.blob_session)?;
        self.worker.write(batch).await?;
        self.state.complete();
        Ok(())
    }

    async fn commit(mut self: Box<Self>) -> Result<WriteSummary> {
        let validation = self
            .state
            .check()
            .and_then(|()| self.blob_session.ensure_idle());
        self.blob_session.fail();
        validation?;
        self.blob_session.cleanup().await?;
        let fragments = self.worker.finish().await?;
        // Share manifest publication, index creation, and failure semantics with
        // ordinary writers. Once publication starts, retain files on failure.
        Box::new(LanceCommitter {
            destination: self.destination,
            options: self.options,
            storage_version: self.storage_version,
        })
        .commit(vec![fragments])
        .await
    }

    async fn abort(mut self: Box<Self>) -> Result<()> {
        self.blob_session.fail();
        // Stop native writing and retain persisted output.
        self.worker.abort().await;
        Ok(())
    }
}

impl BlobDatasetWriter {
    pub(crate) fn new(
        schemas: crate::write_options::LanceSchemas,
        destination: LanceDestination,
        options: WriteOptions,
        params: WriteParams,
    ) -> Self {
        let storage_version = params.data_storage_version;
        let worker = FragmentWorker::new(schemas.encoded.clone(), &destination, params);
        Self {
            schemas,
            blob_session: Arc::new(crate::blob::BlobSession::new(destination.storage.clone())),
            worker,
            state: Poison::default(),
            destination,
            options,
            storage_version,
        }
    }
}
