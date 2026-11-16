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
/// Construct with [`Self::default`] and the `with_*` methods, or mutate fields.
/// The default advertises no supported operations and no input-order guarantee.
///
/// ```compile_fail,E0639
/// use open_arrow_sink_core::{Ordering, SinkCapability};
/// let capability = SinkCapability {
///     ordering: Ordering::Unordered,
///     ..SinkCapability::default()
/// };
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
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

impl Default for SinkCapability {
    /// Advertise no write modes or optional features, and no input-order guarantee.
    /// Sinks must explicitly opt into the modes and features they implement.
    fn default() -> Self {
        Self {
            supports_blob_streaming: false,
            supported_modes: &[],
            ordering: Ordering::Unordered,
            compression: CompressionCapability::default(),
        }
    }
}

impl SinkCapability {
    /// Set `supports_blob_streaming` while preserving all other options.
    pub fn with_supports_blob_streaming(mut self, supports_blob_streaming: bool) -> Self {
        self.supports_blob_streaming = supports_blob_streaming;
        self
    }
    /// Set `supported_modes` while preserving all other options.
    pub fn with_supported_modes(mut self, supported_modes: &'static [crate::WriteMode]) -> Self {
        self.supported_modes = supported_modes;
        self
    }
    /// Set `ordering` while preserving all other options.
    pub fn with_ordering(mut self, ordering: Ordering) -> Self {
        self.ordering = ordering;
        self
    }
    /// Set `compression` while preserving all other options.
    pub fn with_compression(mut self, compression: CompressionCapability) -> Self {
        self.compression = compression;
        self
    }
}
