use std::sync::Arc;

use datafusion_physical_plan::{SendableRecordBatchStream, stream::RecordBatchStreamAdapter};
use futures::stream;
use lance::dataset::{InsertBuilder, transaction::Transaction};
use open_arrow_sink_core::{
    BlobPlacement, BlobWriter, Error, Poison, RecordBatch, Result, SchemaRef, SessionCommitter,
    WriteOptions, WriteSummary, Writer, async_trait, validation,
};
use tokio::{sync::mpsc, task::JoinHandle};

use crate::{
    LanceFragments, committer::LanceCommitter, destination::LanceDestination, error::worker_stopped,
};

pub(crate) struct BlobDatasetWriter {
    schema: SchemaRef,
    encoding_schema: SchemaRef,
    blob_session: Arc<crate::blob::BlobSession>,
    sender: Option<mpsc::Sender<RecordBatch>>,
    task: JoinHandle<lance::Result<Transaction>>,
    rows: u64,
    state: Poison,
    destination: LanceDestination,
    options: WriteOptions,
}

#[async_trait]
impl Writer for BlobDatasetWriter {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    async fn write(&mut self, batch: RecordBatch) -> Result<()> {
        self.write_with_blobs(batch, Vec::new()).await
    }

    async fn create_blob(&mut self) -> Result<Box<dyn BlobWriter>> {
        self.state.check()?;
        self.blob_session.check()?;
        self.state.poison();
        let blob = self.blob_session.create_blob(&self.destination.url).await?;
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
        validation::validate_batch(&self.schema, &batch)?;
        let rows = batch.num_rows() as u64;
        let batch =
            crate::blob::storage_batch(batch, &self.encoding_schema, blobs, &self.blob_session)?;
        self.sender
            .as_ref()
            .unwrap()
            .send(batch)
            .await
            .map_err(|_| worker_stopped())?;
        self.rows += rows;
        self.state.complete();
        Ok(())
    }

    async fn commit(mut self: Box<Self>) -> Result<WriteSummary> {
        if let Err(error) = self
            .state
            .check()
            .and_then(|()| self.blob_session.ensure_idle())
        {
            let _ = self.abort().await;
            return Err(error);
        }
        if let Err(error) = self.blob_session.cleanup().await {
            let _ = self.abort().await;
            return Err(error);
        }
        // Closing the input only finishes fragments; the committer publishes below.
        drop(self.sender.take());
        let transaction = self
            .task
            .await
            .map_err(Error::backend)?
            .map_err(Error::backend)?;
        // Share manifest publication, index creation, and failure semantics with
        // ordinary writers. Once publication starts, retain files on failure.
        Box::new(LanceCommitter {
            destination: self.destination,
            options: self.options,
        })
        .commit(vec![LanceFragments {
            rows_written: self.rows,
            transaction,
        }])
        .await
    }

    async fn abort(mut self: Box<Self>) -> Result<()> {
        self.blob_session.fail();
        drop(self.sender.take());
        // Let native writing stop before cleaning up our new destination.
        let _ = self.task.await;
        if self.options.cleanup_on_error {
            let blobs = self.blob_session.abort().await;
            let directory = self.destination.cleanup().await;
            return blobs.and(directory);
        }
        Ok(())
    }
}

impl BlobDatasetWriter {
    pub(crate) fn new(
        schema: SchemaRef,
        encoding_schema: SchemaRef,
        destination: LanceDestination,
        options: WriteOptions,
    ) -> Result<Self> {
        let (sender, receiver) = mpsc::channel(1);
        let batches = stream::unfold(receiver, |mut receiver| async move {
            receiver.recv().await.map(|batch| (Ok(batch), receiver))
        });
        let stream = Box::pin(RecordBatchStreamAdapter::new(
            encoding_schema.clone(),
            batches,
        )) as SendableRecordBatchStream;
        let params = destination.bind_write_params(crate::write_options::write_params(&options)?);
        let uri = destination.url.to_string();
        let task = tokio::spawn(async move {
            InsertBuilder::new(uri.as_str())
                .with_params(&params)
                .execute_uncommitted_stream(stream)
                .await
        });
        Ok(Self {
            schema,
            encoding_schema,
            blob_session: Arc::new(crate::blob::BlobSession::new(
                destination.storage.clone(),
                &destination.url,
                destination.io_tracker.clone(),
            )),
            sender: Some(sender),
            task,
            rows: 0,
            state: Poison::default(),
            destination,
            options,
        })
    }
}
