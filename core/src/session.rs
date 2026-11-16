use crate::{RecordBatch, Result, SchemaRef, WriteSummary, async_trait};

/// Independent physical writer. Finishing data must not publish table/dataset
/// metadata. The writer remains owned by the caller until session publication,
/// so it can still be aborted after another writer fails to finish.
#[async_trait]
pub trait DataWriter: Send {
    type Output: Send + 'static;
    fn schema(&self) -> &SchemaRef;
    /// Validate the fixed schema, keep buffering bounded, and make write errors
    /// or cancellation terminal. Success means accepted, not published.
    async fn write(&mut self, batch: RecordBatch) -> Result<()>;
    /// Called once, after all accepted writes complete. A failure is terminal.
    async fn finish(&mut self) -> Result<Self::Output>;
    /// Stop I/O and apply the backend cleanup policy, including finished output.
    async fn abort(self: Box<Self>) -> Result<()>;
}

/// Backend-owned publication boundary for all writers in one session.
#[async_trait]
pub trait SessionCommitter<T: Send + 'static>: Send {
    /// What this backend guarantees before `commit` is invoked. Transactional
    /// metadata backends can return `NotPublished`; direct-file sinks must keep
    /// `Unknown`, because completed files may already be visible to readers.
    fn pre_commit_outcome(&self) -> crate::PublicationOutcome {
        crate::PublicationOutcome::Unknown
    }

    /// Called exactly once, after every data writer finishes successfully.
    /// Owns publication-failure cleanup. Publication may have succeeded despite
    /// an error, so callers must never abort writers after entering this method.
    async fn commit(self: Box<Self>, outputs: Vec<T>) -> Result<WriteSummary>;
    /// Called after all data writers have stopped and been aborted. Must also
    /// clean up session resources when an individual writer's abort failed.
    async fn abort(self: Box<Self>) -> Result<()>;
}

/// Writers and their shared publication authority, created by the backend.
/// Drop never publishes or performs asynchronous cleanup.
pub struct WriteSession<T: Send + 'static> {
    pub writers: Vec<Box<dyn DataWriter<Output = T>>>,
    pub committer: Box<dyn SessionCommitter<T>>,
}

impl<T: Send + 'static> WriteSession<T> {
    /// Adapt a one-writer session to the public writer interface. Failed writes,
    /// cancellation, and finish errors abort both the data writer and session.
    /// Once publication starts, cleanup belongs exclusively to the committer.
    pub async fn into_writer(mut self) -> Result<Box<dyn crate::Writer>> {
        if self.writers.len() != 1 {
            for writer in self.writers {
                let _ = writer.abort().await;
            }
            let _ = self.committer.abort().await;
            return Err(crate::Error::Unsupported("expected one data writer".into()));
        }
        Ok(Box::new(SessionWriter {
            writer: self.writers.pop().unwrap(),
            committer: self.committer,
            failed: false,
        }))
    }
}

struct SessionWriter<T: Send + 'static> {
    writer: Box<dyn DataWriter<Output = T>>,
    committer: Box<dyn SessionCommitter<T>>,
    failed: bool,
}

#[async_trait]
impl<T: Send + 'static> crate::Writer for SessionWriter<T> {
    fn schema(&self) -> &SchemaRef {
        self.writer.schema()
    }

    async fn write(&mut self, batch: RecordBatch) -> Result<()> {
        if self.failed {
            return Err(crate::Error::Unsupported(
                "writer failed; abort or drop it".into(),
            ));
        }
        self.failed = true;
        self.writer.write(batch).await?;
        self.failed = false;
        Ok(())
    }

    async fn commit(mut self: Box<Self>) -> Result<WriteSummary> {
        let output = if self.failed {
            Err(crate::Error::Unsupported(
                "writer failed; abort or drop it".into(),
            ))
        } else {
            self.writer.finish().await
        };
        match output {
            Ok(output) => self.committer.commit(vec![output]).await,
            Err(error) => {
                let outcome = self.committer.pre_commit_outcome();
                let _ = self.abort().await;
                Err(error.with_publication_outcome(outcome))
            }
        }
    }

    async fn abort(self: Box<Self>) -> Result<()> {
        let writer = self.writer.abort().await;
        let session = self.committer.abort().await;
        writer.and(session)
    }
}
