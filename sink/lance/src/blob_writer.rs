use crate::config::SessionOptions;
use std::sync::{Arc, atomic::Ordering};

use arrow_array::RecordBatchReader;
use arrow_schema::ArrowError;

use lance::Dataset;
use open_arrow_sink_core::{
    BlobPlacement, BlobWriter, Error, Poison, RecordBatch, Result, SchemaRef, WriteSummary, Writer,
    async_trait, validation,
};
use tokio::{sync::mpsc, task::JoinHandle};

use crate::{
    destination::LanceDestination,
    error::{aborted, worker_stopped},
    index::build_indexes,
};

enum Message {
    Batch(RecordBatch),
    Commit,
}

struct ChannelReader {
    schema: SchemaRef,
    receiver: mpsc::Receiver<Message>,
    finished: bool,
}

impl Iterator for ChannelReader {
    type Item = std::result::Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished {
            return None;
        }
        // Lance polls RecordBatchReader on its blocking background iterator.
        match self.receiver.blocking_recv() {
            Some(Message::Batch(batch)) => Some(Ok(batch)),
            Some(Message::Commit) => {
                self.finished = true;
                None
            }
            None => {
                self.finished = true;
                Some(Err(aborted()))
            }
        }
    }
}

impl RecordBatchReader for ChannelReader {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

pub(crate) struct BlobDatasetWriter {
    schema: SchemaRef,
    encoding_schema: SchemaRef,
    blob_session: Arc<crate::blob::BlobSession>,
    sender: Option<mpsc::Sender<Message>>,
    task: JoinHandle<lance::Result<Dataset>>,
    rows: u64,
    state: Poison,
    destination: LanceDestination,
    options: SessionOptions,
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
        crate::validation::validate_blob_state(&self.state, &self.blob_session)?;
        self.state.poison();
        let blob =
            crate::blob::create_blob(&self.destination.url, self.blob_session.clone()).await?;
        self.state.complete();
        Ok(blob)
    }

    async fn write_with_blobs(
        &mut self,
        batch: RecordBatch,
        blobs: Vec<BlobPlacement>,
    ) -> Result<()> {
        crate::validation::validate_blob_state(&self.state, &self.blob_session)?;
        self.state.poison();
        validation::validate_batch(&self.schema, &batch)?;
        let rows = batch.num_rows() as u64;
        let batch =
            crate::blob::storage_batch(batch, &self.encoding_schema, blobs, &self.blob_session)?;
        self.sender
            .as_ref()
            .unwrap()
            .send(Message::Batch(batch))
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
        let sender = self.sender.take().unwrap();
        // Channel closure is an error; only this marker permits native publication.
        let _ = sender.send(Message::Commit).await;
        drop(sender);
        let mut dataset = self
            .task
            .await
            .map_err(Error::backend)?
            .map_err(Error::backend)?;
        build_indexes(&mut dataset, &self.options.indexes)
            .await
            .map_err(Error::backend)?;
        Ok(WriteSummary {
            rows_written: self.rows,
        })
    }

    async fn abort(mut self: Box<Self>) -> Result<()> {
        self.blob_session.failed.store(true, Ordering::Release);
        drop(self.sender.take());
        // Let native writing stop before removing only our reserved directory.
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
        options: SessionOptions,
    ) -> Self {
        let (sender, receiver) = mpsc::channel(1);
        let reader = ChannelReader {
            schema: encoding_schema.clone(),
            receiver,
            finished: false,
        };
        let params = destination.bind_write_params(crate::write_options::write_params(&options));
        let uri = destination.url.to_string();
        let task =
            tokio::spawn(async move { Dataset::write(reader, uri.as_str(), Some(params)).await });
        Self {
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
        }
    }
}
