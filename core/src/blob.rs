use std::{any::Any, fmt, num::NonZeroUsize};

use crate::{Result, async_trait};

/// Strategy for external blob URIs, matching Lance's write options.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum ExternalBlobMode {
    /// Keep the source URI as an external reference.
    Reference,
    /// Copy the source bytes into destination-owned storage.
    #[default]
    Ingest,
}

/// Per-write blob handle configuration. Currently follows Lance blob options.
/// Native logical blob inputs use these layout and ingestion options. Lance
/// streamed handles also honor an explicitly configured inline threshold;
/// larger streams use dedicated objects rather than packed storage.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct BlobHandleConfig {
    pub external_blob_mode: ExternalBlobMode,
    /// Maximum payload size in bytes kept inline before using packed storage.
    /// Matches Lance BlobFieldOptions; None keeps field metadata or Lance's default
    /// for native logical inputs. For streamed Lance handles, Some opts into bounded
    /// buffering up to this limit; None preserves constant-memory dedicated writes.
    pub inline_size_threshold: Option<usize>,
    /// Maximum payload size in bytes kept in packed storage before using a
    /// dedicated object. Lance selects dedicated storage strictly above this size.
    /// None keeps field metadata or Lance's default; zero is not valid.
    pub dedicated_size_threshold: Option<NonZeroUsize>,
}

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
/// Handles can be attached to a batch using `Writer::write_with_blobs`.
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
