//! Bounded native fragment input and ownership of its background task.
use crate::{LanceFragments, destination::LanceDestination, error::worker_stopped};
use datafusion_physical_plan::{SendableRecordBatchStream, stream::RecordBatchStreamAdapter};
use futures::stream;
use lance::dataset::{InsertBuilder, WriteParams, transaction::Transaction};
use open_arrow_sink_core::{Error, RecordBatch, Result, SchemaRef};
use tokio::{sync::mpsc, task::JoinHandle};

// The enclosing writer guards all writes and finish with Poison. Once its
// sender or task is taken, that writer is terminal.
pub(crate) struct FragmentTask {
    sender: Option<mpsc::Sender<RecordBatch>>,
    task: Option<JoinHandle<lance::Result<Transaction>>>,
}
impl FragmentTask {
    pub fn new(schema: SchemaRef, destination: &LanceDestination, params: WriteParams) -> Self {
        let (sender, receiver) = mpsc::channel(1);
        // EOF only finishes fragments. Manifest publication belongs to the committer.
        let batches = stream::unfold(receiver, |mut receiver| async move {
            receiver.recv().await.map(|batch| (Ok(batch), receiver))
        });
        let stream =
            Box::pin(RecordBatchStreamAdapter::new(schema, batches)) as SendableRecordBatchStream;
        let params = destination.bind_write_params(params);
        let uri = destination.url.to_string();
        let task = tokio::spawn(async move {
            InsertBuilder::new(uri.as_str())
                .with_params(&params)
                .execute_uncommitted_stream(stream)
                .await
        });
        Self {
            sender: Some(sender),
            task: Some(task),
        }
    }
    pub async fn write(&mut self, batch: RecordBatch) -> Result<()> {
        if self
            .sender
            .as_ref()
            .expect("fragment task is open while its writer is usable")
            .send(batch)
            .await
            .is_err()
        {
            // Receiver closure belongs to the worker. Preserve its native error
            // and retain task ownership across cancellation while joining it.
            return Err(self.finish().await.err().unwrap_or_else(worker_stopped));
        }
        Ok(())
    }
    pub async fn finish(&mut self) -> Result<LanceFragments> {
        drop(self.sender.take());
        // Keep ownership across cancellation so abort/drop can still stop the task.
        let result = self
            .task
            .as_mut()
            .expect("fragment task is present until its writer finishes")
            .await;
        self.task.take();
        Ok(LanceFragments {
            transaction: result.map_err(Error::backend)?.map_err(Error::backend)?,
        })
    }
    pub async fn abort(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
            drop(self.sender.take());
            let _ = task.await;
        }
    }
}
impl Drop for FragmentTask {
    fn drop(&mut self) {
        if let Some(task) = &self.task {
            task.abort();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn closed_input_returns_the_native_worker_error() {
        let (sender, receiver) = mpsc::channel(1);
        let task = tokio::spawn(async move {
            drop(receiver);
            Err(lance::Error::invalid_input("injected fragment failure"))
        });
        let mut worker = FragmentTask {
            sender: Some(sender),
            task: Some(task),
        };
        let batch = RecordBatch::new_empty(std::sync::Arc::new(arrow_schema::Schema::empty()));
        // Wait for receiver closure rather than racing the background task.
        worker.sender.as_ref().unwrap().closed().await;
        let error = worker.write(batch).await.err().unwrap();
        let Error::Backend(source) = error else {
            panic!("expected the native backend error");
        };
        assert!(source.is::<lance::Error>());
        assert!(source.to_string().contains("injected fragment failure"));
        worker.abort().await;
    }
}
