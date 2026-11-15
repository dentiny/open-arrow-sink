use std::collections::BTreeMap;

/// Portable compression codec identifiers used to advertise backend capabilities.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompressionCodec {
    Uncompressed,
    Snappy,
    Gzip,
    Zstd,
    Lz4Raw,
}

/// Compression requested for a global default or a column override.
/// Levels left unspecified preserve the backend's codec default.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Compression {
    Uncompressed,
    Snappy,
    Gzip,
    /// Zstandard compression with an optional explicit level.
    /// Backends must reject unsupported levels before destination I/O,
    /// rather than silently clamping them or using a default.
    Zstd {
        level: Option<i32>,
    },
    Lz4Raw,
}

impl Compression {
    /// Codec identifier, independent of encoding parameters.
    pub const fn codec(self) -> CompressionCodec {
        match self {
            Self::Uncompressed => CompressionCodec::Uncompressed,
            Self::Snappy => CompressionCodec::Snappy,
            Self::Gzip => CompressionCodec::Gzip,
            Self::Zstd { .. } => CompressionCodec::Zstd,
            Self::Lz4Raw => CompressionCodec::Lz4Raw,
        }
    }
}

impl From<CompressionCodec> for Compression {
    fn from(codec: CompressionCodec) -> Self {
        match codec {
            CompressionCodec::Uncompressed => Self::Uncompressed,
            CompressionCodec::Snappy => Self::Snappy,
            CompressionCodec::Gzip => Self::Gzip,
            CompressionCodec::Zstd => Self::Zstd { level: None },
            CompressionCodec::Lz4Raw => Self::Lz4Raw,
        }
    }
}

/// Session compression policy. Empty options preserve backend defaults.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CompressionOptions {
    /// Default compression for every column; None preserves the backend default.
    pub global: Option<Compression>,
    /// Overrides keyed by exact top-level Arrow field name (dots are literal).
    /// For nested fields, applies to all descendant physical columns.
    pub columns: BTreeMap<String, Compression>,
}

/// Independently supported codecs for global defaults and column overrides.
/// An empty list means that scope is unsupported, including Uncompressed.
/// Codec support does not imply support for every level; backends validate levels.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CompressionCapability {
    pub global: &'static [CompressionCodec],
    pub per_column: &'static [CompressionCodec],
}
