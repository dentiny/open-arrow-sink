use crate::{CompressionOptions, IndexSpec, PartitionType};
use std::num::NonZeroUsize;

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
    #[default]
    PreserveInput,
    /// Allow batches and rows to be reordered, without loss or duplication.
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

/// Session options. Start from [`Self::default`] and use the `with_*` methods
/// or mutate public fields; this allows future options to receive defaults.
///
/// ```
/// use open_arrow_sink_core::{Ordering, WriteMode, WriteOptions};
/// let mut options = WriteOptions::default()
///     .with_mode(WriteMode::Append)
///     .with_ordering(Ordering::Unordered);
/// options.target_file_size = Some(64 * 1024 * 1024);
/// ```
///
/// External struct literals are deliberately unsupported, including updates:
///
/// ```compile_fail,E0639
/// use open_arrow_sink_core::{WriteMode, WriteOptions};
/// let options = WriteOptions { mode: WriteMode::Append, ..WriteOptions::default() };
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
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
    /// Upload part size in bytes for this session. None preserves storage defaults.
    /// The storage backend may enforce a larger minimum part size.
    pub upload_part_size: Option<NonZeroUsize>,
    /// Concurrent upload requests per writer. None preserves storage defaults.
    /// This is independent of the session's writer count and aggregate HTTP budget.
    pub upload_concurrency: Option<NonZeroUsize>,
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
            upload_part_size: None,
            upload_concurrency: None,
            target_file_size: None,
            storage_version: None,
            cleanup_on_error: true,
        }
    }
}

impl WriteOptions {
    /// Set `io_metrics` while preserving all other options.
    pub fn with_io_metrics(mut self, io_metrics: Option<crate::IoMetrics>) -> Self {
        self.io_metrics = io_metrics;
        self
    }
    /// Set `blob_handle` while preserving all other options.
    pub fn with_blob_handle(mut self, blob_handle: Option<crate::BlobHandleConfig>) -> Self {
        self.blob_handle = blob_handle;
        self
    }
    /// Set `mode` while preserving all other options.
    pub fn with_mode(mut self, mode: WriteMode) -> Self {
        self.mode = mode;
        self
    }
    /// Set `ordering` while preserving all other options.
    pub fn with_ordering(mut self, ordering: Ordering) -> Self {
        self.ordering = ordering;
        self
    }
    /// Set `compression` while preserving all other options.
    pub fn with_compression(mut self, compression: CompressionOptions) -> Self {
        self.compression = compression;
        self
    }
    /// Set `partition` while preserving all other options.
    pub fn with_partition(mut self, partition: Option<PartitionOptions>) -> Self {
        self.partition = partition;
        self
    }
    /// Set `indexes` while preserving all other options.
    pub fn with_indexes(mut self, indexes: Vec<IndexSpec>) -> Self {
        self.indexes = indexes;
        self
    }
    /// Override the storage upload part size for this session.
    pub fn with_upload_part_size(mut self, size: Option<NonZeroUsize>) -> Self {
        self.upload_part_size = size;
        self
    }
    /// Override the storage upload concurrency for each writer in this session.
    pub fn with_upload_concurrency(mut self, concurrency: Option<NonZeroUsize>) -> Self {
        self.upload_concurrency = concurrency;
        self
    }
    /// Set `target_file_size` while preserving all other options.
    pub fn with_target_file_size(mut self, target_file_size: Option<usize>) -> Self {
        self.target_file_size = target_file_size;
        self
    }
    /// Set `storage_version` while preserving all other options.
    pub fn with_storage_version(mut self, storage_version: Option<String>) -> Self {
        self.storage_version = storage_version;
        self
    }
    /// Set `cleanup_on_error` while preserving all other options.
    pub fn with_cleanup_on_error(mut self, cleanup_on_error: bool) -> Self {
        self.cleanup_on_error = cleanup_on_error;
        self
    }
}
