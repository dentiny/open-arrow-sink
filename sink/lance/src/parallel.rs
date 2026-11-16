use arrow_array::RecordBatchReader;
use arrow_schema::ArrowError;
use lance::dataset::{InsertBuilder, transaction::Transaction};
use open_arrow_sink_core::{
    DataWriter, RecordBatch, Result, SchemaRef, WriteOptions, async_trait, validation,
};
use tokio::{sync::mpsc, task::JoinHandle};

use crate::{
    compression::writer_batch,
    error::{aborted, backend, failed, worker_stopped},
    storage::NativeStorage,
};

type Output = (u64, Transaction);

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
        options: &WriteOptions,
    ) -> Result<Self> {
        let (sender, receiver) = mpsc::channel(1);
        let reader = ChannelReader {
            schema: encoding_schema.clone(),
            receiver,
            finished: false,
        };
        let params = destination.bind_write_params(crate::write_options::write_params(options)?);
        let uri = destination.url.to_string();
        let task = tokio::spawn(async move {
            InsertBuilder::new(destination.write_destination(&uri))
                .with_params(&params)
                .execute_uncommitted_stream(
                    Box::new(reader) as Box<dyn arrow_array::RecordBatchReader + Send>
                )
                .await
        });
        Ok(Self {
            schema,
            encoding_schema,
            sender: Some(sender),
            task: Some(task),
            rows: 0,
            failed: false,
        })
    }
}

#[async_trait]
impl DataWriter for FragmentWriter {
    type Output = Output;
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }
    async fn write(&mut self, batch: RecordBatch) -> Result<()> {
        crate::validation::validate_write_state(self.failed || self.sender.is_none())?;
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
        crate::validation::validate_write_state(self.failed)?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{ArrayRef, Int64Array};
    use lance::dataset::{WriteMode as NativeWriteMode, WriteParams};
    use open_arrow_sink_core::{Ordering, WriteMode};
    use open_arrow_sink_storage::{Storage, StorageResolver, Url};
    use std::sync::Arc;

    #[tokio::test]
    async fn all_fragment_writers_use_the_preflight_dataset_version() {
        let temp = tempfile::tempdir().unwrap();
        let url = Url::from_directory_path(temp.path().join("dataset")).unwrap();
        let batch = RecordBatch::try_from_iter([(
            "value",
            Arc::new(Int64Array::from(vec![1])) as ArrayRef,
        )])
        .unwrap();
        let initial = InsertBuilder::new(url.as_str())
            .execute(vec![batch.clone()])
            .await
            .unwrap();
        let initial_version = initial.version().version;
        let storage = Storage::from_url(&url).unwrap();
        let mut destination =
            NativeStorage::new(storage, url.clone(), StorageResolver::default(), None).unwrap();
        destination
            .prepare(&batch.schema(), WriteMode::Append, None)
            .await
            .unwrap();
        let options = WriteOptions {
            mode: WriteMode::Append,
            ordering: Ordering::Unordered,
            ..Default::default()
        };
        let mut first = FragmentWriter::new(
            batch.schema(),
            batch.schema(),
            destination.clone(),
            &options,
        )
        .unwrap();
        first.write(batch.clone()).await.unwrap();
        let (_, first_transaction) = first.finish().await.unwrap();

        // A different session publishes between workers resolving their context.
        InsertBuilder::new(url.as_str())
            .with_params(&WriteParams {
                mode: NativeWriteMode::Append,
                ..Default::default()
            })
            .execute(vec![batch.clone()])
            .await
            .unwrap();
        let mut second =
            FragmentWriter::new(batch.schema(), batch.schema(), destination, &options).unwrap();
        second.write(batch).await.unwrap();
        let (_, second_transaction) = second.finish().await.unwrap();
        assert_eq!(first_transaction.read_version, initial_version);
        assert_eq!(second_transaction.read_version, initial_version);
        crate::validation::validate_transaction_compatibility(
            &first_transaction,
            &second_transaction,
        )
        .unwrap();
    }
}
