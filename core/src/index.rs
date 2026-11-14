//! Backend-independent index requests and construction results.

/// Logical index type. Each backend chooses its concrete implementation
/// and validates supported Arrow types before writing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexType {
    Scalar,
    Vector,
    Text,
    BloomFilter,
}

/// Request an index on one column. At most one index per column per write.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexSpec {
    pub column: String,
    pub index_type: IndexType,
}

impl IndexSpec {
    pub fn new(column: impl Into<String>, index_type: IndexType) -> Self {
        Self {
            column: column.into(),
            index_type,
        }
    }
}

/// Construction details after data publication. Optional version numbers use
/// the backend's native interpretation.
#[derive(Debug, Clone)]
pub struct IndexBuildReport {
    pub destination: String,
    pub rows_written: u64,
    pub data_version: Option<u64>,
    pub latest_version: Option<u64>,
}
