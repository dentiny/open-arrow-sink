use crate::Ordering;

/// Partition transforms a sink can write. Additional transforms can be added
/// as backend support is implemented.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PartitionType {
    Identity,
}

/// Guarantees provided by the configured sink.
/// Each sink must explicitly declare its ordering guarantee.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SinkCapability {
    /// `PreserveInput` guarantees input row order in the target's documented
    /// read order; `Unordered` provides no input-order guarantee.
    /// Sequential I/O completion alone does not guarantee logical row order.
    pub ordering: Ordering,
    /// Partition transforms accepted by this sink. An empty list means
    /// `WriteOptions.partition` is unsupported.
    pub supported_partitions: Vec<PartitionType>,
}
