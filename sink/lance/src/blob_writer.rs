use std::sync::{Arc, atomic::Ordering};

use lance::Dataset;
use open_arrow_sink_core::{
    BlobPlacement, BlobWriter, RecordBatch, Result, SchemaRef, WriteOptions, WriteSummary, Writer,
    async_trait, validation,
};
use tokio::{sync::mpsc, task::JoinHandle};

use crate::{
    compression::writer_batch,
    error::{backend, worker_stopped},
    index::build_indexes,
    parallel::{ChannelReader, Message},
    storage::NativeStorage,
};

pub(crate) struct BlobDatasetWriter {
    schema: SchemaRef,
    encoding_schema: SchemaRef,
    storage_schema: SchemaRef,
    blob_session: Arc<crate::blob::BlobSession>,
    sender: Option<mpsc::Sender<Message>>,
    task: JoinHandle<lance::Result<Dataset>>,
    rows: u64,
    failed: bool,
    destination: NativeStorage,
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
        crate::validation::validate_blob_state(self.failed, &self.blob_session)?;
        self.failed = true;
        let blob =
            crate::blob::create_blob(&self.destination.url, self.blob_session.clone()).await?;
        self.failed = false;
        Ok(blob)
    }

    async fn write_with_blobs(
        &mut self,
        batch: RecordBatch,
        blobs: Vec<BlobPlacement>,
    ) -> Result<()> {
        crate::validation::validate_blob_state(self.failed, &self.blob_session)?;
        self.failed = true;
        validation::validate_batch(&self.schema, &batch)?;
        let rows = batch.num_rows() as u64;
        let batch =
            crate::blob::storage_batch(batch, &self.storage_schema, blobs, &self.blob_session)?;
        let batch = writer_batch(batch, &self.encoding_schema)?;
        self.sender
            .as_ref()
            .unwrap()
            .send(Message::Batch(batch))
            .await
            .map_err(|_| worker_stopped())?;
        self.rows += rows;
        self.failed = false;
        Ok(())
    }

    async fn commit(mut self: Box<Self>) -> Result<WriteSummary> {
        if let Err(error) = crate::validation::validate_blob_commit(self.failed, &self.blob_session)
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
        let mut dataset = self.task.await.map_err(backend)?.map_err(backend)?;
        build_indexes(&mut dataset, &self.options.indexes)
            .await
            .map_err(backend)?;
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
            let blobs = self.blob_session.cleanup().await;
            let directory = self.destination.cleanup().await;
            return blobs.and(directory);
        }
        Ok(())
    }
}

impl BlobDatasetWriter {
    pub(crate) fn new(
        schema: SchemaRef,
        storage_schema: SchemaRef,
        encoding_schema: SchemaRef,
        destination: NativeStorage,
        options: WriteOptions,
    ) -> Result<Self> {
        let (sender, receiver) = mpsc::channel(1);
        let reader = ChannelReader {
            schema: encoding_schema.clone(),
            receiver,
            finished: false,
        };
        let params = destination.write_params(&options)?;
        let uri = destination.url.to_string();
        let task =
            tokio::spawn(async move { Dataset::write(reader, uri.as_str(), Some(params)).await });
        Ok(Self {
            schema,
            storage_schema,
            encoding_schema,
            blob_session: Arc::new(crate::blob::BlobSession::new(destination.storage.clone())),
            sender: Some(sender),
            task,
            rows: 0,
            failed: false,
            destination,
            options,
        })
    }
}
