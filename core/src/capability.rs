use crate::{CompressionCapability, Ordering};

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
/// Shared mode, ordering, compression, and streaming guarantees.
/// Backend-specific configuration belongs to each concrete sink.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SinkCapability {
    /// Supports `Writer::create_blob` and `Writer::write_with_blobs` for compatible
    /// schemas. Independent of native logical blob input/configuration support.
    pub supports_blob_streaming: bool,
    /// Explicit destination modes accepted by this sink.
    pub supported_modes: &'static [crate::WriteMode],
    /// `PreserveInput` guarantees input row order in the target's documented
    /// read order; `Unordered` provides no input-order guarantee.
    /// Sequential I/O completion alone does not guarantee logical row order.
    pub ordering: Ordering,
    /// Compression scopes, codecs, and structural encodings accepted by this sink.
    pub compression: CompressionCapability,
}
