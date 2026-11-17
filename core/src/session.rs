use crate::{GuardedWriter, RecordBatch, Result, SchemaRef, async_trait};

/// Independent physical writer. Finishing data must not publish table/dataset
/// metadata. Errors are returned without deleting output or invoking abort.
#[async_trait]
pub trait DataWriter: Send {
    /// Backend metadata produced by finishing this writer, passed to the
    /// session's [`SessionCommitter`] for publication.
    type Output: Send + 'static;

    /// Return the fixed input schema accepted by this physical writer.
    /// Every batch passed to [`Self::write`] must match it, even if empty.
    fn schema(&self) -> &SchemaRef;
    /// Validate the fixed schema, keep buffering bounded, and make write errors
    /// or cancellation terminal. Success means accepted, not published.
    async fn write(&mut self, batch: RecordBatch) -> Result<()>;
    /// Finish accepted writes and return backend metadata for publication by
    /// the session's [`SessionCommitter`], without publishing it here.
    /// Called once, after all accepted writes complete. A failure is terminal.
    async fn finish(&mut self) -> Result<Self::Output>;
    /// Stop I/O without deleting output, including finished output.
    async fn abort(self: Box<Self>) -> Result<()>;
}

/// Backend-owned publication boundary for all writers in one session.
#[async_trait]
pub trait SessionCommitter<T: Send + 'static>: Send {
    /// Called exactly once, after every data writer finishes successfully.
    /// Return actual published row counts aggregated across all outputs; recovery
    /// counts must be local to this session, even when observers are shared.
    /// Errors leave output in place. Publication may have succeeded despite
    /// an error; no automatic rollback or retry is performed.
    async fn commit(self: Box<Self>, outputs: Vec<T>) -> Result<crate::CommitSummary>;
    /// End an explicitly aborted session after all data writers have stopped.
    /// Leave persisted output in place.
    async fn abort(self: Box<Self>) -> Result<()>;
}

/// Writers and their shared publication authority, created by the backend.
/// Drop never publishes or performs asynchronous cleanup.
pub struct WriteSession<T: Send + 'static> {
    pub writers: Vec<Box<dyn DataWriter<Output = T>>>,
    pub committer: Box<dyn SessionCommitter<T>>,
}

impl<T: Send + 'static> WriteSession<T> {
    /// Wrap every data writer while retaining the shared committer.
    /// The wrapper must preserve each writer's backend output type and lifecycle.
    pub fn map_writers(
        self,
        f: impl FnMut(Box<dyn DataWriter<Output = T>>) -> Box<dyn DataWriter<Output = T>>,
    ) -> Self {
        Self {
            writers: self.writers.into_iter().map(f).collect(),
            committer: self.committer,
        }
    }

    /// Finish all writers concurrently, then publish their outputs exactly once.
    /// A finish or publication failure returns directly without invoking abort.
    /// Cancellation drops unfinished work without asynchronous cleanup.
    pub async fn commit(mut self) -> Result<crate::CommitSummary> {
        let outputs =
            futures::future::try_join_all(self.writers.iter_mut().map(|writer| writer.finish()))
                .await?;
        self.committer.commit(outputs).await
    }

    /// Abort every writer, then the committer, retaining persisted output.
    /// All aborts are attempted even after an error; return the first error.
    pub async fn abort(self) -> Result<()> {
        let mut first_error = None;
        for writer in self.writers {
            if let Err(error) = writer.abort().await {
                first_error.get_or_insert(error);
            }
        }
        if let Err(error) = self.committer.abort().await {
            first_error.get_or_insert(error);
        }
        first_error.map_or(Ok(()), Err)
    }

    /// Require exactly `count` writers, each with the requested schema.
    /// Invalid sessions are dropped without invoking abort or deleting output.
    pub fn expect(self, count: usize, schema: &SchemaRef) -> Result<Self> {
        if self.writers.len() != count
            || self.writers.iter().any(|writer| writer.schema() != schema)
        {
            return Err(crate::Error::backend(std::io::Error::other(
                "backend returned an invalid parallel writer session",
            )));
        }
        Ok(self)
    }

    /// Adapt a one-writer session to the public writer interface.
    /// Failures return directly without invoking abort or deleting output.
    pub fn into_writer(mut self) -> Result<Box<dyn crate::Writer>> {
        if self.writers.len() != 1 {
            return Err(crate::Error::Unsupported("expected one data writer".into()));
        }
        Ok(Box::new(GuardedWriter::new(Box::new(SessionWriter {
            writer: self.writers.pop().unwrap(),
            committer: self.committer,
        }))))
    }
}

struct SessionWriter<T: Send + 'static> {
    writer: Box<dyn DataWriter<Output = T>>,
    committer: Box<dyn SessionCommitter<T>>,
}

#[async_trait]
impl<T: Send + 'static> crate::Writer for SessionWriter<T> {
    fn schema(&self) -> &SchemaRef {
        self.writer.schema()
    }

    async fn write(&mut self, batch: RecordBatch) -> Result<()> {
        self.writer.write(batch).await
    }

    async fn commit(self: Box<Self>) -> Result<crate::CommitSummary> {
        WriteSession {
            writers: vec![self.writer],
            committer: self.committer,
        }
        .commit()
        .await
    }

    async fn abort(self: Box<Self>) -> Result<()> {
        WriteSession {
            writers: vec![self.writer],
            committer: self.committer,
        }
        .abort()
        .await
    }
}
