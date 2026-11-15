use arrow_array::RecordBatchReader;
use arrow_schema::ArrowError;
use lance::Dataset;
use open_arrow_sink_core::{
    RecordBatch, Result, SchemaRef, WriteOptions, WriteSummary, Writer, async_trait, validation,
};
use tokio::{sync::mpsc, task::JoinHandle};

use crate::{
    compression::writer_batch,
    error::{aborted, backend, worker_stopped},
    index::build_indexes,
    storage::NativeStorage,
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
    destination: NativeStorage,
    sender: Option<mpsc::Sender<Message>>,
    task: JoinHandle<lance::Result<Dataset>>,
    rows: u64,
    failed: bool,
    options: WriteOptions,
}

#[async_trait]
impl Writer for LanceWriter {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    async fn write(&mut self, batch: RecordBatch) -> Result<()> {
        crate::validation::validate_write_state(self.failed)?;
        self.failed = true;
        validation::validate_batch(&self.schema, &batch)?;
        let rows = batch.num_rows() as u64;
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
        crate::validation::validate_write_state(self.failed)?;
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
        drop(self.sender.take());
        // Let native writing stop before removing only our reserved directory.
        let _ = self.task.await;
        if self.options.cleanup_on_error {
            self.destination.cleanup().await?;
        }
        Ok(())
    }
}

impl LanceWriter {
    pub(crate) fn new(
        schema: SchemaRef,
        encoding_schema: SchemaRef,
        destination: NativeStorage,
        options: WriteOptions,
    ) -> Self {
        let (sender, receiver) = mpsc::channel(1);
        let reader = ChannelReader {
            schema: encoding_schema.clone(),
            receiver,
            finished: false,
        };
        let params = destination.write_params(options.target_file_size);
        let uri = destination.url.to_string();
        let task =
            tokio::spawn(async move { Dataset::write(reader, uri.as_str(), Some(params)).await });
        Self {
            schema,
            encoding_schema,
            destination,
            sender: Some(sender),
            task,
            rows: 0,
            failed: false,
            options,
        }
    }
}
