use crate::{CompressionOptions, PartitionType};

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
    /// Requested partition transform. The sink must support this type, and
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
    /// Destination policy. Defaults to Create for every sink. Unsupported modes,
    /// including the default, fail before destination I/O.
    pub mode: WriteMode,
    pub ordering: Ordering,
    /// Codec and structural encoding defaults/column overrides; empty preserves backend defaults.
    pub compression: CompressionOptions,
    /// Target data-file size in bytes. None keeps the backend default.
    /// A positive soft target: batches/pages/row groups and footers may exceed it.
    pub target_file_size: Option<usize>,
    /// Best-effort removal of session-created output on abort or failed open/commit.
    /// Enabled by default. Drop never performs asynchronous cleanup.
    pub cleanup_on_error: bool,
}

impl Default for WriteOptions {
    fn default() -> Self {
        Self {
            io_metrics: None,
            mode: WriteMode::default(),
            ordering: Ordering::default(),
            compression: CompressionOptions::default(),
            target_file_size: None,
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
    /// Set `target_file_size` while preserving all other options.
    pub fn with_target_file_size(mut self, target_file_size: Option<usize>) -> Self {
        self.target_file_size = target_file_size;
        self
    }
    /// Set `cleanup_on_error` while preserving all other options.
    pub fn with_cleanup_on_error(mut self, cleanup_on_error: bool) -> Self {
        self.cleanup_on_error = cleanup_on_error;
        self
    }
}
