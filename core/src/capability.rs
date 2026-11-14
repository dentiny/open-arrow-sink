use crate::{IndexType, Ordering};

/// Partition transforms a sink can write.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PartitionType {
    Identity,
    Year,
    Month,
    Day,
    Hour,
    /// Hash into buckets; the destination spec supplies the bucket count.
    Bucket,
    /// Truncate values; the destination spec supplies the width.
    Truncate,
    /// Always produce a null partition value.
    Void,
}

/// Guarantees provided by the configured sink.
/// Each sink must explicitly declare its ordering guarantee, partition support, and index support.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SinkCapability {
    /// `PreserveInput` guarantees input row order in the target's documented
    /// read order; `Unordered` provides no input-order guarantee.
    /// Sequential I/O completion alone does not guarantee logical row order.
    pub ordering: Ordering,
    /// Partition transforms accepted by this sink. An empty list means
    /// `WriteOptions.partition` is unsupported.
    pub supported_partitions: Vec<PartitionType>,
    /// Logical index types this sink can construct. Empty means none.
    /// Backends additionally validate column-type compatibility before writing.
    pub supported_indexes: &'static [IndexType],
}
