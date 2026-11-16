use std::{
    pin::Pin,
    task::{Context, Poll},
};

use datafusion_common::{DataFusionError, Result as DataFusionResult};
use datafusion_physical_plan::{RecordBatchStream, SendableRecordBatchStream};
use futures::Stream;
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

/// Async native input: an explicit Commit is the only successful EOF.
/// Waiting for an upstream batch never occupies a blocking-pool thread.
pub(crate) struct ChannelStream {
    pub(crate) schema: SchemaRef,
    pub(crate) receiver: mpsc::Receiver<Message>,
    pub(crate) finished: bool,
}

impl Stream for ChannelStream {
    type Item = DataFusionResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.finished {
            return Poll::Ready(None);
        }
        match self.receiver.poll_recv(cx) {
            Poll::Ready(Some(Message::Batch(batch))) => Poll::Ready(Some(Ok(batch))),
            Poll::Ready(Some(Message::Commit)) => {
                self.finished = true;
                Poll::Ready(None)
            }
            Poll::Ready(None) => {
                self.finished = true;
                Poll::Ready(Some(Err(DataFusionError::from(aborted()))))
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl RecordBatchStream for ChannelStream {
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
        let stream = Box::pin(ChannelStream {
            schema: encoding_schema.clone(),
            receiver,
            finished: false,
        }) as SendableRecordBatchStream;
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
    use std::sync::Arc;

    fn channel_stream() -> (mpsc::Sender<Message>, ChannelStream) {
        let (sender, receiver) = mpsc::channel(1);
        let schema = Arc::new(arrow_schema::Schema::empty());
        (
            sender,
            ChannelStream {
                schema,
                receiver,
                finished: false,
            },
        )
    }

    #[tokio::test(flavor = "current_thread")]
    async fn channel_stream_waits_without_blocking_and_preserves_schema() {
        use futures::{FutureExt, StreamExt};
        let (sender, mut stream) = channel_stream();
        let schema = stream.schema();
        assert!(stream.next().now_or_never().is_none());
        sender
            .send(Message::Batch(RecordBatch::new_empty(schema.clone())))
            .await
            .unwrap();
        assert_eq!(stream.next().await.unwrap().unwrap().schema(), schema);
        assert!(stream.next().now_or_never().is_none());
        sender.send(Message::Commit).await.unwrap();
        assert!(stream.next().await.is_none());
        drop(sender);
        assert!(stream.next().await.is_none());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn channel_stream_closure_without_commit_is_terminal_error() {
        use futures::StreamExt;
        let (sender, mut stream) = channel_stream();
        sender
            .send(Message::Batch(RecordBatch::new_empty(stream.schema())))
            .await
            .unwrap();
        drop(sender);
        assert!(stream.next().await.unwrap().is_ok());
        let error = stream.next().await.unwrap().unwrap_err();
        assert!(
            error
                .to_string()
                .contains("Lance writer aborted or dropped")
        );
        assert!(stream.next().await.is_none());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn channel_stream_requires_commit_even_when_empty() {
        use futures::StreamExt;
        let (sender, mut stream) = channel_stream();
        sender.send(Message::Commit).await.unwrap();
        drop(sender);
        assert!(stream.next().await.is_none());
        let (sender, mut stream) = channel_stream();
        drop(sender);
        assert!(stream.next().await.unwrap().is_err());
        assert!(stream.next().await.is_none());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn channel_stream_keeps_bounded_sender_backpressure() {
        use futures::{FutureExt, StreamExt};
        let (sender, mut stream) = channel_stream();
        sender
            .send(Message::Batch(RecordBatch::new_empty(stream.schema())))
            .await
            .unwrap();
        assert!(sender.send(Message::Commit).now_or_never().is_none());
        assert!(stream.next().await.unwrap().is_ok());
        sender.send(Message::Commit).await.unwrap();
        assert!(stream.next().await.is_none());
    }
}
