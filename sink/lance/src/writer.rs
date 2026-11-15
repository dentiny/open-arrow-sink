use std::{
    io,
    path::PathBuf,
    sync::{Arc, atomic::Ordering},
};

use arrow_array::RecordBatchReader;
use arrow_schema::ArrowError;
use lance::{Dataset, dataset::WriteParams};
use open_arrow_sink_core::{
    BlobPlacement, BlobWriter, IndexSpec, RecordBatch, Result, SchemaRef, WriteSummary, Writer,
    async_trait, validation,
};
use tokio::{sync::mpsc, task::JoinHandle};

use crate::{
    compression::writer_batch,
    error::{aborted, backend, failed, worker_stopped},
    index::build_indexes,
};

pub(crate) enum Message {
    Batch(RecordBatch),
    Commit,
}

pub(crate) struct ChannelReader {
    pub(crate) schema: SchemaRef,
    pub(crate) receiver: mpsc::Receiver<Message>,
    pub(crate) finished: bool,
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

pub(crate) struct LanceWriter {
    schema: SchemaRef,
    encoding_schema: SchemaRef,
    path: PathBuf,
    storage_schema: SchemaRef,
    blob_session: Arc<crate::blob::BlobSession>,
    sender: Option<mpsc::Sender<Message>>,
    task: JoinHandle<lance::Result<Dataset>>,
    rows: u64,
    failed: bool,
    indexes: Vec<IndexSpec>,
}

#[async_trait]
impl Writer for LanceWriter {
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
        build_indexes(&mut dataset, &self.indexes)
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
        tokio::fs::remove_dir_all(&self.path).await.map_err(backend)
    }
}

impl LanceWriter {
    pub(crate) fn new(
        schema: SchemaRef,
        storage_schema: SchemaRef,
        encoding_schema: SchemaRef,
        path: PathBuf,
        uri: String,
        target_file_size: Option<usize>,
        indexes: Vec<IndexSpec>,
    ) -> Self {
        let (sender, receiver) = mpsc::channel(1);
        let reader = ChannelReader {
            schema: encoding_schema.clone(),
            receiver,
            finished: false,
        };
        let mut params = WriteParams::default();
        if let Some(size) = target_file_size {
            params.max_bytes_per_file = size;
        }
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
            indexes,
        }
    }
}
