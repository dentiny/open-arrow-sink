//! Backend-independent index requests.

/// Logical index type. Each backend chooses its concrete implementation
/// and validates supported Arrow types before writing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum IndexType {
    Scalar,
    Vector,
    Text,
    BloomFilter,
}

/// Request one index kind on a column. Distinct kinds may share a column;
/// duplicate (column, index type) requests in one write are rejected.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexSpec {
    pub column: String,
    pub index_type: IndexType,
    pub existing: ExistingIndexPolicy,
}

impl IndexSpec {
    pub fn with_existing_policy(mut self, policy: ExistingIndexPolicy) -> Self {
        self.existing = policy;
        self
    }

    pub fn new(column: impl Into<String>, index_type: IndexType) -> Self {
        Self {
            column: column.into(),
            index_type,
            existing: ExistingIndexPolicy::default(),
        }
    }
}

/// How an explicit request treats an existing index with the same generated name.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum ExistingIndexPolicy {
    /// Reject at open when possible, before appending data.
    #[default]
    Error,
    /// Keep the existing index. Newly appended fragments may remain unindexed.
    Keep,
    /// Rebuild the index over the committed dataset, replacing the old index.
    Rebuild,
}
