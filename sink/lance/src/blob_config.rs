use std::num::NonZeroUsize;

/// Lance strategy for external blob URIs.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum ExternalBlobMode {
    /// Keep the source URI as an external reference.
    Reference,
    /// Copy the source bytes into destination-owned storage.
    #[default]
    Ingest,
}

/// Per-write Lance blob configuration.
/// These options govern native logical blob inputs; streamed handles already
/// belong to the destination session and do not need external ingestion.
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
