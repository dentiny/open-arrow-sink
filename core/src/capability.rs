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
    /// Supports native logical blob inputs and explicit per-write blob configuration.
    /// Writer/schema and parallel-session restrictions still apply.
    pub supports_blob_handle: bool,
    /// Explicit destination modes accepted by this sink.
    pub supported_modes: &'static [crate::WriteMode],
    /// `PreserveInput` guarantees input row order in the target's documented
    /// read order; `Unordered` provides no input-order guarantee.
    /// Sequential I/O completion alone does not guarantee logical row order.
    pub ordering: Ordering,
    /// Compression scopes, codecs, and structural encodings accepted by this sink.
    pub compression: CompressionCapability,
    /// Partition transforms accepted by this sink. An empty list means
    /// `WriteOptions.partition` is unsupported.
    pub supported_partitions: Vec<PartitionType>,
    /// Whether session upload part-size and concurrency overrides are accepted.
    pub supports_upload_options: bool,
    /// Whether an explicit `WriteOptions.storage_version` is accepted.
    pub supports_storage_version: bool,
    /// Logical index types this sink can construct. Empty means none.
    /// Backends additionally validate column-type compatibility before writing.
    pub supported_indexes: &'static [IndexType],
}

impl SinkCapability {
    /// Set `supports_blob_handle` while preserving all other options.
    pub fn with_supports_blob_handle(mut self, supports_blob_handle: bool) -> Self {
        self.supports_blob_handle = supports_blob_handle;
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
    /// Set `supported_partitions` while preserving all other options.
    pub fn with_supported_partitions(mut self, supported_partitions: Vec<PartitionType>) -> Self {
        self.supported_partitions = supported_partitions;
        self
    }
    /// Set `supports_upload_options` while preserving all other options.
    pub fn with_supports_upload_options(mut self, supports_upload_options: bool) -> Self {
        self.supports_upload_options = supports_upload_options;
        self
    }
    /// Set `supports_storage_version` while preserving all other options.
    pub fn with_supports_storage_version(mut self, supports_storage_version: bool) -> Self {
        self.supports_storage_version = supports_storage_version;
        self
    }
    /// Set `supported_indexes` while preserving all other options.
    pub fn with_supported_indexes(mut self, supported_indexes: &'static [IndexType]) -> Self {
        self.supported_indexes = supported_indexes;
        self
    }
}

impl Default for SinkCapability {
    /// Construct conservative capabilities: no modes, codecs, partitions,
    /// indexes, or blob APIs, and no ordering guarantee.
    fn default() -> Self {
        Self {
            supports_blob_handle: false,
            supported_modes: &[],
            ordering: Ordering::Unordered,
            compression: CompressionCapability::default(),
            supported_partitions: Vec::new(),
            supports_upload_options: false,
            supports_storage_version: false,
            supported_indexes: &[],
        }
    }
}
