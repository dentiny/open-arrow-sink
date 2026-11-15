use std::{
    io,
    path::PathBuf,
    sync::{Arc, atomic::Ordering},
};

use lance::Dataset;
use open_arrow_sink_core::{
    BlobPlacement, BlobWriter, RecordBatch, Result, SchemaRef, WriteOptions, WriteSummary, Writer,
    async_trait, validation,
};
use tokio::{sync::mpsc, task::JoinHandle};

use crate::{
    compression::writer_batch,
    error::{backend, failed, worker_stopped},
    index::build_indexes,
    parallel::{ChannelReader, Message},
    storage::NativeStorage,
};

pub(crate) struct BlobDatasetWriter {
    schema: SchemaRef,
    encoding_schema: SchemaRef,
    path: PathBuf,
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

    fn supports_blob_streams(&self) -> bool {
        true
    }

    async fn create_blob(&mut self) -> Result<Box<dyn BlobWriter>> {
        if self.failed || self.blob_session.failed.load(Ordering::Relaxed) {
            return Err(failed());
        }
        self.failed = true;
        let blob = crate::blob::create_blob(&self.path, self.blob_session.clone()).await?;
        self.failed = false;
        Ok(blob)
    }

    async fn write_with_blobs(
        &mut self,
        batch: RecordBatch,
        blobs: Vec<BlobPlacement>,
    ) -> Result<()> {
        if self.failed || self.blob_session.failed.load(Ordering::Relaxed) {
            return Err(failed());
        }
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
        if self.failed
            || self.blob_session.failed.load(Ordering::Relaxed)
            || self.blob_session.active.load(Ordering::Relaxed) != 0
        {
            let _ = self.abort().await;
            return Err(backend(io::Error::other(
                "writer failed or has unfinished blobs; session aborted",
            )));
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
        self.blob_session.failed.store(true, Ordering::Relaxed);
        drop(self.sender.take());
        // Let native writing stop before removing only our reserved directory.
        let _ = self.task.await;
        if self.options.cleanup_on_error {
            self.destination.cleanup().await?;
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
        path: PathBuf,
        options: WriteOptions,
    ) -> Self {
        let (sender, receiver) = mpsc::channel(1);
        let reader = ChannelReader {
            schema: encoding_schema.clone(),
            receiver,
            finished: false,
        };
        let params = destination.write_params(&options);
        let uri = destination.url.to_string();
        let task =
            tokio::spawn(async move { Dataset::write(reader, uri.as_str(), Some(params)).await });
        Self {
            schema,
            storage_schema,
            encoding_schema,
            blob_session: Arc::new(crate::blob::BlobSession::default()),
            path,
            sender: Some(sender),
            task,
            rows: 0,
            failed: false,
            destination,
            options,
        }
    }
}
