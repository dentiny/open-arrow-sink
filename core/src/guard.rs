//! Reusable terminal-state enforcement for writer decorators.
use crate::{
    BlobPlacement, BlobWriter, DataWriter, Poison, RecordBatch, Result, SchemaRef, WriteSummary,
    Writer, async_trait,
};

/// Enforce the writer lifecycle around validation, transformation, and I/O.
/// Errors or cancellation of any write/blob-open operation are terminal. A
/// poisoned commit aborts the inner writer; publication errors remain owned by
/// the inner commit implementation. Blob lifetimes remain backend-owned.
pub struct GuardedWriter {
    inner: Box<dyn Writer>,
    state: Poison,
}

impl GuardedWriter {
    pub fn new(inner: Box<dyn Writer>) -> Self {
        Self {
            inner,
            state: Poison::default(),
        }
    }

    /// Check before starting an observation of a new operation.
    pub fn check(&self) -> Result<()> {
        self.state.check()
    }
}

#[async_trait]
impl Writer for GuardedWriter {
    fn schema(&self) -> &SchemaRef {
        self.inner.schema()
    }

    async fn write(&mut self, batch: RecordBatch) -> Result<()> {
        self.state.begin()?;
        self.inner.write(batch).await?;
        self.state.complete();
        Ok(())
    }

    async fn write_with_blobs(
        &mut self,
        batch: RecordBatch,
        blobs: Vec<BlobPlacement>,
    ) -> Result<()> {
        self.state.begin()?;
        self.inner.write_with_blobs(batch, blobs).await?;
        self.state.complete();
        Ok(())
    }

    async fn create_blob(&mut self) -> Result<Box<dyn BlobWriter>> {
        self.state.begin()?;
        let blob = self.inner.create_blob().await?;
        self.state.complete();
        Ok(blob)
    }

    async fn commit(self: Box<Self>) -> Result<WriteSummary> {
        if let Err(error) = self.check() {
            let _ = self.inner.abort().await;
            return Err(error);
        }
        self.inner.commit().await
    }

    async fn abort(self: Box<Self>) -> Result<()> {
        self.inner.abort().await
    }
}

/// Enforce terminal writes and one-shot finish while retaining abort ownership.
/// Each independent data writer gets its own guard; finishing one writer must
/// not poison another writer in the same parallel session.
pub struct GuardedDataWriter<T: Send + 'static> {
    inner: Box<dyn DataWriter<Output = T>>,
    state: Poison,
}

impl<T: Send + 'static> GuardedDataWriter<T> {
    pub fn new(inner: Box<dyn DataWriter<Output = T>>) -> Self {
        Self {
            inner,
            state: Poison::default(),
        }
    }

    /// Check before starting an observation of a new operation.
    pub fn check(&self) -> Result<()> {
        self.state.check()
    }
}

#[async_trait]
impl<T: Send + 'static> DataWriter for GuardedDataWriter<T> {
    type Output = T;

    fn schema(&self) -> &SchemaRef {
        self.inner.schema()
    }

    async fn write(&mut self, batch: RecordBatch) -> Result<()> {
        self.state.begin()?;
        self.inner.write(batch).await?;
        self.state.complete();
        Ok(())
    }

    async fn finish(&mut self) -> Result<T> {
        self.state.begin()?;
        self.inner.finish().await
    }

    async fn abort(self: Box<Self>) -> Result<()> {
        self.inner.abort().await
    }
}
