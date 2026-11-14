//! Mandatory shared contracts, separate from optional business validation layers.
use std::collections::HashSet;

use crate::{Error, IndexSpec, Ordering, RecordBatch, Result, SinkCapability, WriteOptions};
use arrow_schema::Schema;

/// Check declared capabilities before opening a destination or producing side effects.
pub fn validate_options(options: &WriteOptions, capability: SinkCapability) -> Result<()> {
    for index in &options.indexes {
        if !capability.supported_indexes.contains(&index.index_type) {
            return Err(Error::Unsupported(format!(
                "{:?} index construction on column {}",
                index.index_type, index.column
            )));
        }
    }
    if options.target_file_size == Some(0) {
        return Err(Error::Unsupported(
            "target_file_size must be greater than zero".into(),
        ));
    }
    if options.ordering == Ordering::PreserveInput && capability.ordering != Ordering::PreserveInput
    {
        return Err(Error::Unsupported("preserving input row order".into()));
    }
    Ok(())
}

/// Exact schema equality, including field order, nullability and metadata.
/// Casting, schema reconciliation and evolution require an explicit adapter.
pub fn validate_batch(schema: &Schema, batch: &RecordBatch) -> Result<()> {
    if schema != batch.schema().as_ref() {
        return Err(Error::SchemaMismatch);
    }
    Ok(())
}

/// Validate index column existence and uniqueness independently of the backend.
/// Concrete backends additionally validate their supported column types.
pub fn validate_indexes(schema: &Schema, indexes: &[IndexSpec]) -> Result<()> {
    let mut columns = HashSet::new();
    for index in indexes {
        if !columns.insert(&index.column) {
            return Err(Error::Unsupported(format!(
                "multiple indexes requested for column {}",
                index.column
            )));
        }
        schema
            .field_with_name(&index.column)
            .map_err(|_| Error::Unsupported(format!("index column not found: {}", index.column)))?;
    }
    Ok(())
}
