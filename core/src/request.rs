/// Ordering within one session; this does not sort rows by column values.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum Ordering {
    #[default]
    PreserveInput,
    /// Allow batches and rows to be reordered, without loss or duplication.
    Unordered,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct WriteOptions {
    pub ordering: Ordering,
}
