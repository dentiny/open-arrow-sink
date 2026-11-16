use crate::{CompressionOptions, IndexSpec, PartitionType};

/// How a session treats its destination. Support is declared by each sink.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum WriteMode {
    /// Create a new destination; fail if it already exists.
    #[default]
    Create,
    /// Add rows to an existing destination; fail if it does not exist.
    Append,
    /// Replace all rows, or create the destination if it does not exist.
    /// This is a full replacement, not a partition overwrite. Atomicity is
    /// backend-specific; consult the sink documentation.
    Overwrite,
}

/// Input-order policy or guarantee; this does not sort rows by column values.
/// In `WriteOptions` this is requested policy; in `SinkCapability` it is a guarantee.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum Ordering {
    /// Require input row order in the backend's documented read order.
    PreserveInput,
    /// Allow batches and rows to be reordered, without loss or duplication.
    #[default]
    Unordered,
}

/// Enable partition routing using the destination's partition specification.
/// Currently supported only by the Iceberg sink.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PartitionOptions {
    /// Requested partition transform. The sink must advertise this type, and
    /// every field in the destination spec must use it; this does not alter
    /// the table spec. Bucket counts and truncate widths come from that spec.
    pub partition_type: PartitionType,
}

impl Default for PartitionOptions {
    fn default() -> Self {
        Self {
            partition_type: PartitionType::Identity,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriteOptions {
    /// Optional shared I/O observer. Layers forward it; instrumented backends
    /// record measured payloads independently of commit success.
    pub io_metrics: Option<crate::IoMetrics>,
    /// Native blob options for this session. None uses BlobHandleConfig defaults
    /// on supporting backends, including Ingest for external blob URIs.
    /// Explicit configuration requires `SinkCapability.supports_blob_handle`.
    pub blob_handle: Option<crate::BlobHandleConfig>,
    /// Destination policy. Defaults to Create for every sink. Unsupported modes,
    /// including the default, fail before destination I/O.
    pub mode: WriteMode,
    pub ordering: Ordering,
    /// Codec and structural encoding defaults/column overrides; empty preserves backend defaults.
    pub compression: CompressionOptions,
    /// None disables partition routing; Some uses the destination partition spec.
    pub partition: Option<PartitionOptions>,
    /// Indexes to construct after writing. Empty by default; unsupported sinks
    /// reject nonempty requests before opening a writer.
    pub indexes: Vec<IndexSpec>,
    /// Target data-file size in bytes. None keeps the backend default.
    /// A positive soft target: batches/pages/row groups and footers may exceed it.
    pub target_file_size: Option<usize>,
    /// Backend data-file format version. None preserves the backend default.
    /// Currently supported only by Lance; values use the backend version syntax.
    pub storage_version: Option<String>,
    /// Best-effort removal of session-created output on abort or failed open/commit.
    /// Enabled by default. Drop never performs asynchronous cleanup.
    pub cleanup_on_error: bool,
}

impl Default for WriteOptions {
    fn default() -> Self {
        Self {
            io_metrics: None,
            blob_handle: None,
            mode: WriteMode::default(),
            ordering: Ordering::default(),
            compression: CompressionOptions::default(),
            partition: None,
            indexes: Vec::new(),
            target_file_size: None,
            storage_version: None,
            cleanup_on_error: true,
        }
    }
}
