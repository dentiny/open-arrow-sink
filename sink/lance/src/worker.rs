//! Bounded native fragment input and ownership of its background task.
use crate::{LanceFragments, destination::LanceDestination, error::worker_stopped};
use datafusion_physical_plan::{SendableRecordBatchStream, stream::RecordBatchStreamAdapter};
use futures::stream;
use lance::dataset::{InsertBuilder, WriteDestination, WriteParams, transaction::Transaction};
use open_arrow_sink_core::{Error, RecordBatch, Result, SchemaRef};
use tokio::{sync::mpsc, task::JoinHandle};

// The enclosing writer guards all writes and finish with Poison. Once its
// sender or task is taken, that writer is terminal.
pub(crate) struct FragmentTask {
    rows: u64,
    sender: Option<mpsc::Sender<RecordBatch>>,
    task: Option<JoinHandle<lance::Result<Transaction>>>,
}
impl FragmentTask {
    pub fn new(
        schema: SchemaRef,
        destination: &LanceDestination,
        existing: Option<std::sync::Arc<lance::Dataset>>,
        params: WriteParams,
    ) -> Self {
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
            // Share the snapshot validated when opening the write session.
            let destination = existing.map_or(
                WriteDestination::Uri(uri.as_str()),
                WriteDestination::Dataset,
            );
            InsertBuilder::new(destination)
                .with_params(&params)
                .execute_uncommitted_stream(stream)
                .await
        });
        Self {
            rows: 0,
            sender: Some(sender),
            task: Some(task),
        }
    }
    pub async fn write(&mut self, batch: RecordBatch) -> Result<()> {
        let rows = batch.num_rows() as u64;
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
        self.rows += rows;
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
            summary: open_arrow_sink_core::CommitSummary {
                rows_committed: self.rows,
                ..Default::default()
            },
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

    #[rstest::rstest]
    #[case::append(lance::dataset::WriteMode::Append)]
    #[case::overwrite(lance::dataset::WriteMode::Overwrite)]
    #[tokio::test]
    async fn fragment_workers_share_the_prepared_dataset_version(
        #[case] mode: lance::dataset::WriteMode,
    ) {
        use open_arrow_sink_storage::{BlobDownloader, StorageResolver};
        use open_arrow_sink_test_support::{
            int64_batch, memory_warehouse, unordered_options, url, write_batches,
        };
        use std::sync::Arc;

        let storage = memory_warehouse();
        let sink = crate::LanceDatasetSink::new(storage.clone());
        let url = url(&storage, "shared-snapshot/");
        let batch = int64_batch("value", vec![Some(1)]);
        write_batches(
            &sink,
            url.clone(),
            std::slice::from_ref(&batch),
            unordered_options(),
        )
        .await;
        let destination = LanceDestination::new(
            storage,
            url,
            Arc::new(StorageResolver::default()),
            Arc::new(BlobDownloader::with_defaults().unwrap()),
            None,
        )
        .unwrap();
        let existing = Arc::new(destination.dataset_builder().load().await.unwrap());
        let read_version = existing.version().version;
        let params = WriteParams {
            mode: lance::dataset::WriteMode::Append,
            ..Default::default()
        };
        // Another writer advances latest after this session's preparation.
        let advanced = InsertBuilder::new(existing.clone())
            .with_params(&destination.bind_write_params(params.clone()))
            .execute(vec![batch.clone()])
            .await
            .unwrap();
        assert!(advanced.version().version > read_version);
        for _ in 0..2 {
            let mut worker = FragmentTask::new(
                batch.schema(),
                &destination,
                Some(existing.clone()),
                WriteParams {
                    mode,
                    ..params.clone()
                },
            );
            worker.write(batch.clone()).await.unwrap();
            let output = worker.finish().await.unwrap();
            assert_eq!(output.transaction.read_version, read_version);
        }
        // Fragment workers never publish their transactions themselves.
        assert_eq!(
            destination
                .dataset_builder()
                .load()
                .await
                .unwrap()
                .version()
                .version,
            advanced.version().version
        );
    }

    #[tokio::test]
    async fn closed_input_returns_the_native_worker_error() {
        let (sender, receiver) = mpsc::channel(1);
        let task = tokio::spawn(async move {
            drop(receiver);
            Err(lance::Error::invalid_input("injected fragment failure"))
        });
        let mut worker = FragmentTask {
            rows: 0,
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
