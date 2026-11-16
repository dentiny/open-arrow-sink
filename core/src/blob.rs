use std::num::NonZeroUsize;

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
/// These options govern native logical blob inputs. The backend SDK owns physical layout.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct BlobHandleConfig {
    pub external_blob_mode: ExternalBlobMode,
    /// Maximum payload size in bytes kept inline before using packed storage.
    /// Matches Lance BlobFieldOptions; None keeps field metadata or Lance's default.
    pub inline_size_threshold: Option<usize>,
    /// Maximum payload size in bytes kept in packed storage before using a
    /// dedicated object. Lance selects dedicated storage strictly above this size.
    /// None keeps field metadata or Lance's default; zero is not valid.
    pub dedicated_size_threshold: Option<NonZeroUsize>,
}

/// A download chunk and the resources accounting for its buffered bytes.
/// Keep the chunk alive until its bytes have been consumed by the downloader.
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
