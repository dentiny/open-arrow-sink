use std::{collections::BTreeMap, ops::RangeInclusive};

/// Explicit positive Zstandard compression levels supported by the sink adapters.
/// A backend may additionally accept 0 to select its native default level.
pub const ZSTD_COMPRESSION_LEVEL_RANGE: RangeInclusive<i32> = 1..=22;

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
pub enum CompressionSpec {
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

impl CompressionSpec {
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

impl From<CompressionCodec> for CompressionSpec {
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

/// Physical organization of encoded column values, independent of the codec.
/// Backend support is advertised separately from compression codecs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StructuralEncoding {
    Miniblock,
    Fullzip,
}

/// Session codec and structural encoding policy. Empty options preserve backend defaults.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CompressionOptions {
    /// Default compression for every column; None preserves the backend default.
    pub global: Option<CompressionSpec>,
    /// Overrides keyed by exact top-level Arrow field name (dots are literal).
    /// For nested fields, applies to all descendant physical columns.
    pub columns: BTreeMap<String, CompressionSpec>,
    /// Explicit nested field paths, omitting list item names. Applied after top-level overrides.
    /// Segments are literal names, so dots never make a path ambiguous.
    pub nested_columns: BTreeMap<Vec<String>, CompressionSpec>,
    /// Default structural encoding. None preserves backend/schema defaults.
    /// Independent of the global codec and per-column codec overrides.
    pub structural_encoding: Option<StructuralEncoding>,
    /// Structural overrides for exact, unique top-level column names.
    /// Each override replaces only the structural default, not the codec.
    /// Both policies apply recursively to nested fields.
    pub column_encodings: BTreeMap<String, StructuralEncoding>,
}

/// Supported codec scopes and structural encodings.
/// An empty list means that scope is unsupported, including Uncompressed.
/// Codec support does not imply support for every level; backends validate levels.
/// Construct with [`Self::default`] and the `with_*` methods, or mutate fields.
///
/// ```compile_fail,E0639
/// use open_arrow_sink_core::CompressionCapability;
/// let capability = CompressionCapability {
///     nested_columns: true,
///     ..CompressionCapability::default()
/// };
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct CompressionCapability {
    pub global: &'static [CompressionCodec],
    pub per_column: &'static [CompressionCodec],
    pub nested_columns: bool,
    /// Supported global and per-column structural encodings. Empty means unsupported.
    pub structural_encodings: &'static [StructuralEncoding],
}

impl CompressionCapability {
    /// Set `global` while preserving all other options.
    pub fn with_global(mut self, global: &'static [CompressionCodec]) -> Self {
        self.global = global;
        self
    }
    /// Set `per_column` while preserving all other options.
    pub fn with_per_column(mut self, per_column: &'static [CompressionCodec]) -> Self {
        self.per_column = per_column;
        self
    }
    /// Set `nested_columns` while preserving all other options.
    pub fn with_nested_columns(mut self, nested_columns: bool) -> Self {
        self.nested_columns = nested_columns;
        self
    }
    /// Set `structural_encodings` while preserving all other options.
    pub fn with_structural_encodings(
        mut self,
        structural_encodings: &'static [StructuralEncoding],
    ) -> Self {
        self.structural_encodings = structural_encodings;
        self
    }
}
