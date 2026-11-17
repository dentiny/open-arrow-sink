//! Structured diagnostics from a sink's I/O-free preflight.

/// One independently actionable schema or options problem.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaIssue {
    /// Literal field-name segments, including nested container fields.
    /// Empty for a session-wide option or schema problem.
    pub path: Vec<String>,
    pub kind: SchemaIssueKind,
    pub message: String,
}

impl SchemaIssue {
    pub fn new(path: Vec<String>, kind: SchemaIssueKind, message: impl Into<String>) -> Self {
        Self {
            path,
            kind,
            message: message.into(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum SchemaIssueKind {
    InvalidName,
    ReservedName,
    Duplicate,
    UnsupportedType,
    BlobType,
    IndexType,
    MissingColumn,
    InvalidOption,
}
