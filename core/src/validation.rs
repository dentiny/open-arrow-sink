//! Mandatory shared contracts, separate from optional business validation layers.
use std::collections::HashSet;

use crate::{Error, IndexSpec, Ordering, RecordBatch, Result, SinkCapability, WriteOptions};
use arrow_schema::Schema;

use crate::error::{
    duplicate_index, invalid_target_file_size, missing_index_column, preserve_input_unsupported,
    unsupported_index, unsupported_partition,
};

/// Check declared capabilities before opening a destination or producing side effects.
pub fn validate_options(options: &WriteOptions, capability: SinkCapability) -> Result<()> {
    if let Some(partition) = options.partition
        && !capability
            .supported_partitions
            .contains(&partition.partition_type)
    {
        return Err(unsupported_partition(partition.partition_type));
    }
    for index in &options.indexes {
        if !capability.supported_indexes.contains(&index.index_type) {
            return Err(unsupported_index(index));
        }
    }
    if options.target_file_size == Some(0) {
        return Err(invalid_target_file_size());
    }
    if options.ordering == Ordering::PreserveInput && capability.ordering != Ordering::PreserveInput
    {
        return Err(preserve_input_unsupported());
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
            return Err(duplicate_index(&index.column));
        }
        schema
            .field_with_name(&index.column)
            .map_err(|_| missing_index_column(&index.column))?;
    }
    Ok(())
}
