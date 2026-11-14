//! Backend-independent index requests and construction results.

/// Logical index category. Each backend chooses its concrete implementation
/// and validates supported Arrow types before writing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexKind {
    Scalar,
    Vector,
    Text,
    BloomFilter,
}

/// Request an index on one column. At most one index per column per write.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexSpec {
    pub column: String,
    pub kind: IndexKind,
}

impl IndexSpec {
    pub fn new(column: impl Into<String>, kind: IndexKind) -> Self {
        Self {
            column: column.into(),
            kind,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IndexBuildState {
    Pending,
    Complete { index_id: String },
    Failed { error: String },
}

#[derive(Debug, Clone)]
pub struct IndexBuildResult {
    pub spec: IndexSpec,
    pub state: IndexBuildState,
}

/// Construction details after data publication. Identifiers and optional
/// version numbers use the backend's native interpretation.
#[derive(Debug, Clone)]
pub struct IndexBuildReport {
    pub destination: String,
    pub rows_written: u64,
    pub data_version: Option<u64>,
    pub latest_version: Option<u64>,
    pub indexes: Vec<IndexBuildResult>,
}
