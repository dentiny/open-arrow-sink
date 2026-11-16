//! Opt-in streaming blob protocol, separate from the core batch writer contract.
use open_arrow_sink_core::{
    Error, RecordBatch, Result, SchemaRef, Sink, WriteOptions, Writer, async_trait,
};
use std::{any::Any, fmt};

/// A download chunk and the resources accounting for its buffered bytes.
/// Keep the chunk alive until its bytes have been consumed by the blob writer.
pub struct BlobChunk {
    data: bytes::Bytes,
    _guard: Option<Box<dyn Send + Sync>>,
}

impl BlobChunk {
    pub fn new(data: impl Into<bytes::Bytes>) -> Self {
        Self {
            data: data.into(),
            _guard: None,
        }
    }

    /// Borrow the payload while retaining this chunk's accounting reservation.
    pub fn bytes(&self) -> &bytes::Bytes {
        &self.data
    }

    /// Attach an accounting reservation. It is released when this chunk drops.
    pub fn with_guard(data: impl Into<bytes::Bytes>, guard: impl Send + Sync + 'static) -> Self {
        Self {
            data: data.into(),
            _guard: Some(Box::new(guard)),
        }
    }
}

impl AsRef<[u8]> for BlobChunk {
    fn as_ref(&self) -> &[u8] {
        &self.data
    }
}

/// A completed blob owned by one backend write session.
/// Handles can be attached to a batch using `BlobSessionWriter::write_with_blobs`.
pub struct BlobHandle(Box<dyn Any + Send + Sync>);

impl BlobHandle {
    /// Backend adapters use this to retain their native descriptor and ownership.
    pub fn new(value: impl Any + Send + Sync) -> Self {
        Self(Box::new(value))
    }
    pub fn downcast_ref<T: Any>(&self) -> Option<&T> {
        self.0.downcast_ref()
    }
}

impl fmt::Debug for BlobHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("BlobHandle(..)")
    }
}

/// Attach a completed blob to one non-null cell in a blob-marked column.
/// The cell's ordinary binary payload must be empty.
#[derive(Debug)]
pub struct BlobPlacement {
    pub column: usize,
    pub row: usize,
    pub blob: BlobHandle,
}

/// One streaming blob. Drop/cancellation must never publish a partial object.
#[async_trait]
pub trait BlobWriter: Send {
    /// Consume the chunk before returning, retaining its accounting guard until then.
    async fn write(&mut self, chunk: BlobChunk) -> Result<()>;
    async fn finish(self: Box<Self>) -> Result<BlobHandle>;
    async fn abort(self: Box<Self>) -> Result<()>;
}

/// A batch writer that additionally accepts streaming native blobs.
/// An unfinished or failed blob poisons the session. Foreign-session handles,
/// duplicate placements, and nonempty/null placeholder cells must be rejected.
#[async_trait]
pub trait BlobSessionWriter: Writer {
    async fn create_blob(&mut self) -> Result<Box<dyn BlobWriter>>;
    async fn write_with_blobs(
        &mut self,
        batch: RecordBatch,
        blobs: Vec<BlobPlacement>,
    ) -> Result<()>;
}

/// Explicit opt-in to native blob sessions. Ordinary `Sink::open` remains usable
/// for batch writes. Layers must implement this extension to forward streaming.
#[async_trait]
pub trait BlobSink: Sink {
    /// Validate blob schemas/options before destination I/O, as for `Sink::open`.
    /// The default rejects streaming without opening the destination.
    async fn open_blobs(
        &self,
        _request: Self::Request,
        _schema: SchemaRef,
        _options: WriteOptions,
    ) -> Result<Box<dyn BlobSessionWriter>> {
        Err(Error::Unsupported(
            "sink does not support streaming blobs".into(),
        ))
    }
}
