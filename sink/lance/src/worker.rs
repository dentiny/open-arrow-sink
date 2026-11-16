//! Bounded native fragment input and ownership of its background task.
use crate::{destination::LanceDestination, error::worker_stopped};
use datafusion_physical_plan::{SendableRecordBatchStream, stream::RecordBatchStreamAdapter};
use futures::stream;
use lance::dataset::{InsertBuilder, WriteParams, transaction::Transaction};
use open_arrow_sink_core::{Error, RecordBatch, Result, SchemaRef};
use tokio::{sync::mpsc, task::JoinHandle};

pub(crate) struct FragmentWorker {
    sender: Option<mpsc::Sender<RecordBatch>>,
    task: Option<JoinHandle<lance::Result<Transaction>>>,
}
impl FragmentWorker {
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
    pub async fn write(&self, batch: RecordBatch) -> Result<()> {
        self.sender
            .as_ref()
            .ok_or(Error::Poisoned)?
            .send(batch)
            .await
            .map_err(|_| worker_stopped())
    }
    pub async fn finish(&mut self) -> Result<Transaction> {
        drop(self.sender.take());
        // Keep ownership across cancellation so abort/drop can still stop the task.
        let result = self.task.as_mut().ok_or(Error::Poisoned)?.await;
        self.task.take();
        result.map_err(Error::backend)?.map_err(Error::backend)
    }
    pub async fn abort(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
            drop(self.sender.take());
            let _ = task.await;
        }
    }
}
impl Drop for FragmentWorker {
    fn drop(&mut self) {
        if let Some(task) = &self.task {
            task.abort();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{future::Future, task::Poll};
    use tokio::sync::oneshot;

    struct Exited(Option<oneshot::Sender<()>>);
    impl Drop for Exited {
        fn drop(&mut self) {
            let _ = self.0.take().unwrap().send(());
        }
    }
    fn pending_worker() -> (FragmentWorker, oneshot::Receiver<()>) {
        let (exited, observed) = oneshot::channel();
        let guard = Exited(Some(exited));
        let task = tokio::spawn(async move {
            let _guard = guard;
            std::future::pending::<lance::Result<Transaction>>().await
        });
        let (sender, _receiver) = mpsc::channel(1);
        (
            FragmentWorker {
                sender: Some(sender),
                task: Some(task),
            },
            observed,
        )
    }

    #[tokio::test]
    async fn cancelled_finish_keeps_worker_owned_until_abort_stops_it() {
        let (mut worker, exited) = pending_worker();
        {
            let mut finish = Box::pin(worker.finish());
            std::future::poll_fn(|cx| {
                assert!(finish.as_mut().poll(cx).is_pending());
                Poll::Ready(())
            })
            .await;
        }
        assert!(worker.sender.is_none());
        assert!(worker.task.is_some());
        worker.abort().await;
        assert!(worker.task.is_none());
        exited.await.unwrap();
    }

    #[tokio::test]
    async fn drop_stops_native_work_instead_of_detaching_it() {
        let (worker, exited) = pending_worker();
        drop(worker);
        exited.await.unwrap();
    }
}
