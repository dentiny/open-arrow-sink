use crate::Ordering;

/// Guarantees provided by the configured sink.
/// Each sink must explicitly declare its ordering guarantee.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SinkCapability {
    /// `PreserveInput` guarantees input row order in the target's documented
    /// read order; `Unordered` provides no input-order guarantee.
    /// Sequential I/O completion alone does not guarantee logical row order.
    pub ordering: Ordering,
    /// Whether this sink accepts `WriteOptions.partition`.
    pub partition: bool,
}
