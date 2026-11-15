use std::collections::BTreeMap;

/// Portable compression codecs. Backends use each codec's default level.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompressionCodec {
    Uncompressed,
    Snappy,
    Gzip,
    Zstd,
    Lz4Raw,
}

/// Session compression policy. Empty options preserve backend defaults.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CompressionOptions {
    /// Default codec for every column; None preserves the backend default.
    pub global: Option<CompressionCodec>,
    /// Overrides keyed by exact top-level Arrow field name (dots are literal).
    /// For nested fields, applies to all descendant physical columns.
    pub columns: BTreeMap<String, CompressionCodec>,
}

/// Independently supported codecs for global defaults and column overrides.
/// An empty list means that scope is unsupported, including Uncompressed.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CompressionCapability {
    pub global: &'static [CompressionCodec],
    pub per_column: &'static [CompressionCodec],
}
