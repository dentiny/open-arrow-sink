//! Create-only local Lance datasets. Requires a Tokio runtime.
use std::{io, path::PathBuf};

use arrow_array::RecordBatchReader;
use arrow_schema::ArrowError;
use lance::{Dataset, dataset::WriteParams};
use open_arrow_sink_core::{
    Error, Ordering, RecordBatch, Result, SchemaRef, Sink, SinkCapability, WriteOptions,
    WriteSummary, Writer, async_trait, validation,
};
use tokio::{sync::mpsc, task::JoinHandle};

/// Creates a new local dataset directory. Existing paths are always rejected.
/// Only unordered writes are supported; dataset scan order is not guaranteed.
#[derive(Debug, Clone, Copy, Default)]
pub struct LanceDatasetSink;

#[async_trait]
impl Sink for LanceDatasetSink {
    type Request = PathBuf;

    fn capability(&self) -> SinkCapability {
        SinkCapability {
            ordering: Ordering::Unordered,
        }
    }

    async fn open(
        &self,
        path: PathBuf,
        schema: SchemaRef,
        options: WriteOptions,
    ) -> Result<Box<dyn Writer>> {
        validation::validate_options(&options, self.capability())?;
        // Reserve a fresh destination. The caller must keep exclusive ownership of
        // this directory until commit/abort; no existing dataset is overwritten.
        let path = std::path::absolute(path).map_err(backend)?;
        let uri = path
            .to_str()
            .ok_or_else(|| {
                backend(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "Lance requires a UTF-8 path",
                ))
            })?
            .to_owned();
        tokio::fs::create_dir(&path).await.map_err(backend)?;
        let (sender, receiver) = mpsc::channel(1);
        let reader = ChannelReader {
            schema: schema.clone(),
            receiver,
            finished: false,
        };
        let task = tokio::spawn(async move {
            Dataset::write(reader, uri.as_str(), Some(WriteParams::default())).await
        });
        Ok(Box::new(LanceWriter {
            schema,
            path,
            sender: Some(sender),
            task,
            rows: 0,
            failed: false,
        }))
    }
}

enum Message {
    Batch(RecordBatch),
    Commit,
}

struct ChannelReader {
    schema: SchemaRef,
    receiver: mpsc::Receiver<Message>,
    finished: bool,
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
                Some(Err(ArrowError::ExternalError(Box::new(io::Error::new(
                    io::ErrorKind::Interrupted,
                    "Lance writer aborted or dropped",
                )))))
            }
        }
    }
}

impl RecordBatchReader for ChannelReader {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

struct LanceWriter {
    schema: SchemaRef,
    path: PathBuf,
    sender: Option<mpsc::Sender<Message>>,
    task: JoinHandle<lance::Result<Dataset>>,
    rows: u64,
    failed: bool,
}

#[async_trait]
impl Writer for LanceWriter {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    async fn write(&mut self, batch: RecordBatch) -> Result<()> {
        if self.failed {
            return Err(backend(io::Error::other("writer failed; abort or drop it")));
        }
        self.failed = true;
        validation::validate_batch(&self.schema, &batch)?;
        let rows = batch.num_rows() as u64;
        self.sender
            .as_ref()
            .unwrap()
            .send(Message::Batch(batch))
            .await
            .map_err(|_| backend(io::Error::other("Lance write worker stopped")))?;
        self.rows += rows;
        self.failed = false;
        Ok(())
    }

    async fn commit(mut self: Box<Self>) -> Result<WriteSummary> {
        if self.failed {
            return Err(backend(io::Error::other("writer failed; abort or drop it")));
        }
        let sender = self.sender.take().unwrap();
        // Channel closure is an error; only this marker permits native publication.
        let _ = sender.send(Message::Commit).await;
        drop(sender);
        self.task.await.map_err(backend)?.map_err(backend)?;
        Ok(WriteSummary {
            rows_written: self.rows,
        })
    }

    async fn abort(mut self: Box<Self>) -> Result<()> {
        drop(self.sender.take());
        // Let native writing stop before removing only our reserved directory.
        let _ = self.task.await;
        tokio::fs::remove_dir_all(&self.path).await.map_err(backend)
    }
}

fn backend(error: impl std::error::Error + Send + Sync + 'static) -> Error {
    Error::Backend(Box::new(error))
}
