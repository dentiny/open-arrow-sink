use lance::dataset::{InsertBuilder, transaction::Transaction};
use open_arrow_sink_core::{DataWriter, RecordBatch, Result, SchemaRef, async_trait, validation};
use tokio::{sync::mpsc, task::JoinHandle};

use crate::{
    compression::writer_batch,
    error::{backend, failed, worker_stopped},
    storage::NativeStorage,
    writer::{ChannelReader, Message},
};

type Output = (u64, Transaction);

/// Writes uncommitted fragments. No worker can publish a dataset manifest.
pub(crate) struct FragmentWriter {
    schema: SchemaRef,
    encoding_schema: SchemaRef,
    sender: Option<mpsc::Sender<Message>>,
    task: Option<JoinHandle<lance::Result<Transaction>>>,
    rows: u64,
    failed: bool,
}

impl FragmentWriter {
    pub(crate) fn new(
        schema: SchemaRef,
        encoding_schema: SchemaRef,
        destination: NativeStorage,
        target_file_size: Option<usize>,
    ) -> Self {
        let (sender, receiver) = mpsc::channel(1);
        let reader = ChannelReader {
            schema: encoding_schema.clone(),
            receiver,
            finished: false,
        };
        let params = destination.write_params(target_file_size);
        let uri = destination.url.to_string();
        let task = tokio::spawn(async move {
            InsertBuilder::new(uri.as_str())
                .with_params(&params)
                .execute_uncommitted_stream(
                    Box::new(reader) as Box<dyn arrow_array::RecordBatchReader + Send>
                )
                .await
        });
        Self {
            schema,
            encoding_schema,
            sender: Some(sender),
            task: Some(task),
            rows: 0,
            failed: false,
        }
    }
}

#[async_trait]
impl DataWriter for FragmentWriter {
    type Output = Output;
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }
    async fn write(&mut self, batch: RecordBatch) -> Result<()> {
        if self.failed || self.sender.is_none() {
            return Err(failed());
        }
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
    async fn finish(&mut self) -> Result<Output> {
        if self.failed {
            return Err(failed());
        }
        self.failed = true;
        let sender = self.sender.take().ok_or_else(failed)?;
        let _ = sender.send(Message::Commit).await;
        drop(sender);
        let transaction = self
            .task
            .take()
            .ok_or_else(failed)?
            .await
            .map_err(backend)?
            .map_err(backend)?;
        Ok((self.rows, transaction))
    }
    async fn abort(mut self: Box<Self>) -> Result<()> {
        drop(self.sender.take());
        if let Some(task) = self.task.take() {
            let _ = task.await;
        }
        // Session cleanup runs only after every worker has stopped.
        Ok(())
    }
}
