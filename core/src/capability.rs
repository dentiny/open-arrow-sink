use crate::{CompressionCapability, IndexType, Ordering};

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
/// Each sink explicitly declares ordering, partition, index, compression, and blob handle support.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SinkCapability {
    /// Supports native logical blob inputs and explicit per-write blob configuration.
    /// Writer/schema and parallel-session restrictions still apply.
    pub supports_blob_handle: bool,
    /// Supports `Writer::create_blob` and `Writer::write_with_blobs` for compatible
    /// schemas. Independent of native logical blob input/configuration support.
    pub supports_blob_streaming: bool,
    /// Explicit destination modes accepted by this sink.
    pub supported_modes: &'static [crate::WriteMode],
    /// `PreserveInput` guarantees input row order in the target's documented
    /// read order; `Unordered` provides no input-order guarantee.
    /// Sequential I/O completion alone does not guarantee logical row order.
    pub ordering: Ordering,
    /// Compression scopes and codecs accepted by this sink.
    pub compression: CompressionCapability,
    /// Partition transforms accepted by this sink. An empty list means
    /// `WriteOptions.partition` is unsupported.
    pub supported_partitions: Vec<PartitionType>,
    /// Whether an explicit `WriteOptions.storage_version` is accepted.
    pub supports_storage_version: bool,
    /// Logical index types this sink can construct. Empty means none.
    /// Backends additionally validate column-type compatibility before writing.
    pub supported_indexes: &'static [IndexType],
}
