/// Input-order policy or guarantee; this does not sort rows by column values.
/// In `WriteOptions` this is requested policy; in `SinkCapability` it is a guarantee.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum Ordering {
    #[default]
    PreserveInput,
    /// Allow batches and rows to be reordered, without loss or duplication.
    Unordered,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WriteOptions {
    pub ordering: Ordering,
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
            target_file_size: None,
            cleanup_on_error: true,
        }
    }
}
