use datafusion_physical_plan::{SendableRecordBatchStream, stream::RecordBatchStreamAdapter};
use futures::stream;
use lance::dataset::{InsertBuilder, transaction::Transaction};
use open_arrow_sink_core::{
    DataWriter, Error, Poison, RecordBatch, Result, SchemaRef, WriteOptions, async_trait,
    validation,
};
use tokio::{sync::mpsc, task::JoinHandle};

use crate::{
    LanceFragments, compression::writer_batch, error::worker_stopped, storage::NativeStorage,
};

/// Writes uncommitted fragments. No worker can publish a dataset manifest.
pub(crate) struct FragmentWriter {
    schema: SchemaRef,
    encoding_schema: SchemaRef,
    sender: Option<mpsc::Sender<RecordBatch>>,
    task: Option<JoinHandle<lance::Result<Transaction>>>,
    rows: u64,
    state: Poison,
    downloader: std::sync::Arc<open_arrow_sink_storage::BlobDownloader>,
    blob_mode: open_arrow_sink_core::ExternalBlobMode,
    io_metrics: Option<open_arrow_sink_core::IoMetrics>,
}

impl FragmentWriter {
    pub(crate) fn new(
        schema: SchemaRef,
        encoding_schema: SchemaRef,
        destination: NativeStorage,
        options: &WriteOptions,
    ) -> Result<Self> {
        let (sender, receiver) = mpsc::channel(1);
        // Closing the channel ends fragment input; publication belongs to the committer.
        let batches = stream::unfold(receiver, |mut receiver| async move {
            receiver.recv().await.map(|batch| (Ok(batch), receiver))
        });
        let stream = Box::pin(RecordBatchStreamAdapter::new(
            encoding_schema.clone(),
            batches,
        )) as SendableRecordBatchStream;
        let params = destination.bind_write_params(crate::write_options::write_params(options)?);
        let uri = destination.url.to_string();
        let task = tokio::spawn(async move {
            InsertBuilder::new(uri.as_str())
                .with_params(&params)
                .execute_uncommitted_stream(stream)
                .await
        });
        Ok(Self {
            schema,
            encoding_schema,
            sender: Some(sender),
            task: Some(task),
            rows: 0,
            state: Poison::default(),
            downloader: destination.downloader.clone(),
            blob_mode: options
                .blob_handle
                .as_ref()
                .map(|config| config.external_blob_mode)
                .unwrap_or_default(),
            io_metrics: options.io_metrics.clone(),
        })
    }
}

#[async_trait]
impl DataWriter for FragmentWriter {
    type Output = LanceFragments;
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }
    async fn write(&mut self, batch: RecordBatch) -> Result<()> {
        self.state.begin()?;
        validation::validate_batch(&self.schema, &batch)?;
        let rows = batch.num_rows() as u64;
        if crate::blob_input::has_blobs(&self.encoding_schema) {
            crate::blob_input::send_rows(
                &batch,
                &self.encoding_schema,
                &self.downloader,
                self.blob_mode,
                self.io_metrics.as_ref(),
                self.sender.as_ref().unwrap(),
            )
            .await?;
        } else {
            let batch = writer_batch(batch, &self.encoding_schema)?;
            self.sender
                .as_ref()
                .unwrap()
                .send(batch)
                .await
                .map_err(|_| worker_stopped())?;
        }
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
