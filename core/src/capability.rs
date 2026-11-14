use crate::{IndexType, Ordering};

/// Guarantees provided by the configured sink.
/// Each sink must explicitly declare its ordering guarantee and index support.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SinkCapability {
    /// `PreserveInput` guarantees input row order in the target's documented
    /// read order; `Unordered` provides no input-order guarantee.
    /// Sequential I/O completion alone does not guarantee logical row order.
    pub ordering: Ordering,
    /// Logical index types this sink can construct. Empty means none.
    /// Backends additionally validate column-type compatibility before writing.
    pub supported_indexes: &'static [IndexType],
}
