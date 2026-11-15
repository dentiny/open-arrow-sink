//! Backend-independent index requests.

/// Logical index type. Each backend chooses its concrete implementation
/// and validates supported Arrow types before writing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexType {
    Scalar,
    Vector,
    /// Product-quantized vector index. Backends choose the concrete algorithm.
    VectorPq,
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
