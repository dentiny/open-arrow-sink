use crate::{IndexSpec, PartitionType};

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
/// Currently supported only by the Iceberg sink, for identity transforms.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PartitionOptions {
    /// Requested partition transform. The sink must advertise this type, and
    /// the destination spec must use it; this does not alter the table spec.
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
    pub ordering: Ordering,
    /// None disables partition routing; Some uses the destination partition spec.
    pub partition: Option<PartitionOptions>,
    /// Indexes to construct after writing. Empty by default; unsupported sinks
    /// reject nonempty requests before opening a writer.
    pub indexes: Vec<IndexSpec>,
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
            ordering: Ordering::default(),
            partition: None,
            indexes: Vec::new(),
            target_file_size: None,
            cleanup_on_error: true,
        }
    }
}
