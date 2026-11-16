use std::sync::{Arc, atomic::Ordering};

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
    schemas: crate::write_options::LanceSchemas,
    blob_session: Arc<crate::blob::BlobSession>,
    sender: Option<mpsc::Sender<RecordBatch>>,
    task: Option<JoinHandle<lance::Result<Transaction>>>,
    logical: bool,
    rows: u64,
    state: Poison,
    destination: LanceDestination,
    options: WriteOptions,
}

#[async_trait]
impl Writer for BlobDatasetWriter {
    fn schema(&self) -> &SchemaRef {
        &self.schemas.input
    }

    async fn write(&mut self, batch: RecordBatch) -> Result<()> {
        crate::validation::validate_blob_state(&self.state, &self.blob_session)?;
        self.start_worker(true)?;
        if !self.logical {
            return self.write_with_blobs(batch, Vec::new()).await;
        }
        self.state.poison();
        validation::validate_batch(&self.schemas.input, &batch)?;
        let rows = batch.num_rows() as u64;
        let schema = self.schemas.logical.clone();
        crate::blob_input::send_rows(
            &batch,
            &schema,
            &self.destination.downloader,
            self.options
                .blob_handle
                .as_ref()
                .map(|config| config.external_blob_mode)
                .unwrap_or_default(),
            self.options.io_metrics.as_ref(),
            self.sender.as_ref().unwrap(),
        )
        .await?;
        self.rows += rows;
        self.state.complete();
        Ok(())
    }

    async fn create_blob(&mut self) -> Result<Box<dyn BlobWriter>> {
        crate::validation::validate_blob_state(&self.state, &self.blob_session)?;
        self.start_worker(false)?;
        if self.logical {
            self.state.poison();
            return Err(crate::error::mixed_blob_inputs());
        }
        self.state.poison();
        let blob =
            crate::blob::create_blob(&self.destination.path, self.blob_session.clone()).await?;
        self.state.complete();
        Ok(blob)
    }

    async fn write_with_blobs(
        &mut self,
        batch: RecordBatch,
        blobs: Vec<BlobPlacement>,
    ) -> Result<()> {
        if blobs.is_empty() && (self.task.is_none() || self.logical) {
            return self.write(batch).await;
        }
        crate::validation::validate_blob_state(&self.state, &self.blob_session)?;
        self.start_worker(false)?;
        if self.logical {
            self.state.poison();
            return Err(crate::error::mixed_blob_inputs());
        }
        self.state.poison();
        validation::validate_batch(&self.schemas.input, &batch)?;
        let rows = batch.num_rows() as u64;
        let batch =
            crate::blob::storage_batch(batch, &self.schemas.encoded, blobs, &self.blob_session)?;
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
        if let Err(error) = crate::validation::validate_blob_commit(&self.state, &self.blob_session)
        {
            let _ = self.abort().await;
            return Err(error);
        }
        if let Err(error) = self.blob_session.cleanup().await {
            let _ = self.abort().await;
            return Err(error);
        }
        self.start_worker(true)?;
        // Closing the input only finishes fragments; the committer publishes below.
        drop(self.sender.take());
        let transaction = self
            .task
            .take()
            .unwrap()
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
        self.blob_session.failed.store(true, Ordering::Release);
        drop(self.sender.take());
        // Let native writing stop before cleaning up our new destination.
        if let Some(task) = self.task.take() {
            let _ = task.await;
        }
        if self.options.cleanup_on_error {
            let blobs = self.blob_session.abort().await;
            let directory = self.destination.cleanup().await;
            return blobs.and(directory);
        }
        Ok(())
    }
}

impl BlobDatasetWriter {
    fn start_worker(&mut self, logical: bool) -> Result<()> {
        if self.task.is_some() {
            return Ok(());
        }
        let (sender, receiver) = mpsc::channel(1);
        let schema = if logical {
            self.schemas.logical.clone()
        } else {
            self.schemas.encoded.clone()
        };
        let batches = stream::unfold(receiver, |mut receiver| async move {
            receiver.recv().await.map(|batch| (Ok(batch), receiver))
        });
        let stream =
            Box::pin(RecordBatchStreamAdapter::new(schema, batches)) as SendableRecordBatchStream;
        let params = self
            .destination
            .bind_write_params(crate::write_options::write_params(&self.options)?);
        let uri = self.destination.url.to_string();
        let task = tokio::spawn(async move {
            InsertBuilder::new(uri.as_str())
                .with_params(&params)
                .execute_uncommitted_stream(stream)
                .await
        });
        self.sender = Some(sender);
        self.task = Some(task);
        self.logical = logical;
        Ok(())
    }

    pub(crate) fn new(
        schemas: crate::write_options::LanceSchemas,
        destination: LanceDestination,
        options: WriteOptions,
    ) -> Result<Self> {
        Ok(Self {
            schemas,
            blob_session: Arc::new(crate::blob::BlobSession::new(
                destination.storage.clone(),
                &destination.path,
                destination.io_tracker.clone(),
            )),
            sender: None,
            task: None,
            logical: true,
            rows: 0,
            state: Poison::default(),
            destination,
            options,
        })
    }
}
