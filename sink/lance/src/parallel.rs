use datafusion_physical_plan::{SendableRecordBatchStream, stream::RecordBatchStreamAdapter};
use futures::stream;
use lance::dataset::{InsertBuilder, WriteParams, transaction::Transaction};
use open_arrow_sink_core::{
    DataWriter, Error, Poison, RecordBatch, Result, SchemaRef, async_trait, validation,
};
use tokio::{sync::mpsc, task::JoinHandle};

use crate::{
    LanceFragments, compression::writer_batch, destination::LanceDestination, error::worker_stopped,
};

/// Writes uncommitted fragments. No worker can publish a dataset manifest.
pub(crate) struct FragmentWriter {
    schemas: crate::write_options::LanceSchemas,
    sender: Option<mpsc::Sender<RecordBatch>>,
    task: Option<JoinHandle<lance::Result<Transaction>>>,
    rows: u64,
    state: Poison,
}

impl FragmentWriter {
    pub(crate) fn new(
        schemas: crate::write_options::LanceSchemas,
        destination: LanceDestination,
        params: WriteParams,
    ) -> Self {
        let (sender, receiver) = mpsc::channel(1);
        // Closing the channel ends fragment input; publication belongs to the committer.
        let batches = stream::unfold(receiver, |mut receiver| async move {
            receiver.recv().await.map(|batch| (Ok(batch), receiver))
        });
        let stream = Box::pin(RecordBatchStreamAdapter::new(
            schemas.encoded.clone(),
            batches,
        )) as SendableRecordBatchStream;
        let params = destination.bind_write_params(params);
        let uri = destination.url.to_string();
        let task = tokio::spawn(async move {
            InsertBuilder::new(uri.as_str())
                .with_params(&params)
                .execute_uncommitted_stream(stream)
                .await
        });
        Self {
            schemas,
            sender: Some(sender),
            task: Some(task),
            rows: 0,
            state: Poison::default(),
        }
    }
}

#[async_trait]
impl DataWriter for FragmentWriter {
    type Output = LanceFragments;
    fn schema(&self) -> &SchemaRef {
        &self.schemas.input
    }
    async fn write(&mut self, batch: RecordBatch) -> Result<()> {
        self.state.begin()?;
        validation::validate_batch(&self.schemas.input, &batch)?;
        let rows = batch.num_rows() as u64;
        let batch = writer_batch(batch, &self.schemas.encoded)?;
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
    async fn finish(&mut self) -> Result<Self::Output> {
        self.state.begin()?;
        drop(self.sender.take());
        // Retain the worker if this await is cancelled, so abort/drop can stop it.
        let result = self
            .task
            .as_mut()
            .expect("active writer owns its native worker")
            .await;
        self.task.take();
        let transaction = result.map_err(Error::backend)?.map_err(Error::backend)?;
        Ok(LanceFragments {
            rows_written: self.rows,
            transaction,
        })
    }
    async fn abort(mut self: Box<Self>) -> Result<()> {
        if let Some(task) = self.task.take() {
            task.abort();
            drop(self.sender.take());
            let _ = task.await;
        }
        // Session cleanup runs only after every worker has stopped.
        Ok(())
    }
}

impl Drop for FragmentWriter {
    fn drop(&mut self) {
        if let Some(task) = &self.task {
            task.abort();
        }
    }
}
