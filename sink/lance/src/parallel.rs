use futures::stream;
use lance::{
    dataset::{InsertBuilder, transaction::Transaction},
    deps::datafusion::physical_plan::{
        SendableRecordBatchStream, stream::RecordBatchStreamAdapter,
    },
};
use open_arrow_sink_core::{DataWriter, RecordBatch, Result, SchemaRef, async_trait, validation};
use tokio::{sync::mpsc, task::JoinHandle};

use crate::{
    compression::writer_batch,
    error::{backend, failed, worker_stopped},
};

type Output = (u64, Transaction);

/// Writes uncommitted fragments. No worker can publish a dataset manifest.
pub(crate) struct FragmentWriter {
    schema: SchemaRef,
    encoding_schema: SchemaRef,
    sender: Option<mpsc::Sender<RecordBatch>>,
    task: Option<JoinHandle<lance::Result<Transaction>>>,
    rows: u64,
    failed: bool,
}

impl FragmentWriter {
    pub(crate) fn new(
        schema: SchemaRef,
        encoding_schema: SchemaRef,
        uri: String,
        params: lance::dataset::WriteParams,
    ) -> Self {
        let (sender, receiver) = mpsc::channel(1);
        let batches = stream::unfold(receiver, |mut receiver| async move {
            receiver.recv().await.map(|batch| (Ok(batch), receiver))
        });
        let stream: SendableRecordBatchStream = Box::pin(RecordBatchStreamAdapter::new(
            encoding_schema.clone(),
            batches,
        ));
        let task = tokio::spawn(async move {
            InsertBuilder::new(uri.as_str())
                .with_params(&params)
                .execute_uncommitted_stream(stream)
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
        crate::validation::validate_write_state(self.failed || self.sender.is_none())?;
        self.failed = true;
        validation::validate_batch(&self.schema, &batch)?;
        let rows = batch.num_rows() as u64;
        let batch = writer_batch(batch, &self.encoding_schema)?;
        self.sender
            .as_ref()
            .unwrap()
            .send(batch)
            .await
            .map_err(|_| worker_stopped())?;
        self.rows += rows;
        self.failed = false;
        Ok(())
    }
    async fn finish(&mut self) -> Result<Output> {
        crate::validation::validate_write_state(self.failed)?;
        self.failed = true;
        // Closing this stream only finishes uncommitted fragments, never publication.
        drop(self.sender.take().ok_or_else(failed)?);
        // Keep ownership across cancellation so a later abort can stop the worker.
        let result = self.task.as_mut().ok_or_else(failed)?.await;
        self.task.take();
        let transaction = result.map_err(backend)?.map_err(backend)?;
        Ok((self.rows, transaction))
    }
    async fn abort(mut self: Box<Self>) -> Result<()> {
        drop(self.sender.take());
        if let Some(task) = self.task.take() {
            task.abort();
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::{future::pending, sync::Arc, time::Duration};
    use tokio::sync::oneshot;

    struct Stopped(Option<oneshot::Sender<()>>);
    impl Drop for Stopped {
        fn drop(&mut self) {
            if let Some(sender) = self.0.take() {
                let _ = sender.send(());
            }
        }
    }

    #[rstest::rstest]
    #[case::abort(true)]
    #[case::drop(false)]
    #[tokio::test]
    async fn cancelled_finish_retains_worker_until_abort_or_drop(#[case] abort: bool) {
        let (sender, mut receiver) = mpsc::channel(1);
        let (closed, stream_closed) = oneshot::channel();
        let (stopped, worker_stopped) = oneshot::channel();
        let task = tokio::spawn(async move {
            let _stopped = Stopped(Some(stopped));
            while receiver.recv().await.is_some() {}
            let _ = closed.send(());
            // Simulate native finalization that has not completed yet.
            pending::<lance::Result<Transaction>>().await
        });
        let schema = Arc::new(arrow_schema::Schema::empty());
        let mut writer = FragmentWriter {
            encoding_schema: schema.clone(),
            schema,
            sender: Some(sender),
            task: Some(task),
            rows: 0,
            failed: false,
        };
        tokio::select! {
            result = writer.finish() => panic!("finalization unexpectedly ended: {result:?}"),
            result = stream_closed => result.unwrap(),
        }
        // The cancelled finish must leave the worker owned, so either cleanup
        // path can stop it rather than detaching its pending native I/O.
        if abort {
            Box::new(writer).abort().await.unwrap();
        } else {
            drop(writer);
        }
        tokio::time::timeout(Duration::from_secs(1), worker_stopped)
            .await
            .expect("worker remained detached after cleanup")
            .unwrap();
    }
}
