use std::path::PathBuf;

use lance::dataset::{
    CommitBuilder, InsertBuilder, WriteParams,
    transaction::{Operation, Transaction},
};
use open_arrow_sink_core::{
    DataWriter, Error, IndexSpec, RecordBatch, Result, SchemaRef, SessionCommitter, WriteSummary,
    async_trait, validation,
};
use tokio::{sync::mpsc, task::JoinHandle};

use crate::{
    compression::writer_batch,
    error::{backend, failed, worker_stopped},
    index::build_indexes,
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
        uri: String,
        target_file_size: Option<usize>,
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

pub(crate) struct LanceCommitter {
    pub path: PathBuf,
    pub uri: String,
    pub indexes: Vec<IndexSpec>,
    pub cleanup_on_error: bool,
}

#[async_trait]
impl SessionCommitter<Output> for LanceCommitter {
    async fn commit(self: Box<Self>, outputs: Vec<Output>) -> Result<WriteSummary> {
        let mut rows_written = 0;
        let mut combined: Option<Transaction> = None;
        for (rows, transaction) in outputs {
            rows_written += rows;
            if let Some(target) = &mut combined {
                match (&mut target.operation, transaction.operation) {
                    (
                        Operation::Overwrite { fragments, .. },
                        Operation::Overwrite {
                            fragments: incoming,
                            ..
                        },
                    ) => fragments.extend(incoming),
                    _ => {
                        return Err(Error::Unsupported(
                            "expected uncommitted Lance create fragments".into(),
                        ));
                    }
                }
            } else {
                combined = Some(transaction);
            }
        }
        let transaction =
            combined.ok_or_else(|| Error::Unsupported("empty Lance writer session".into()))?;
        // One manifest publication assigns fragment IDs across all workers.
        // Retain files on failure: publication may already have succeeded.
        let mut dataset = CommitBuilder::new(self.uri.as_str())
            .execute(transaction)
            .await
            .map_err(backend)?;
        build_indexes(&mut dataset, &self.indexes)
            .await
            .map_err(backend)?;
        Ok(WriteSummary { rows_written })
    }
    async fn abort(self: Box<Self>) -> Result<()> {
        if self.cleanup_on_error {
            tokio::fs::remove_dir_all(&self.path)
                .await
                .map_err(backend)?;
        }
        Ok(())
    }
}
