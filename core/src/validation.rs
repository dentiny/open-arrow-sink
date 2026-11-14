//! Mandatory shared contracts, separate from optional business validation layers.
use crate::{Error, Ordering, RecordBatch, Result, SinkCapability, WriteOptions};
use arrow_schema::Schema;

use crate::error::{invalid_target_file_size, preserve_input_unsupported};

/// Check declared capabilities before opening a destination or producing side effects.
pub fn validate_options(options: &WriteOptions, capability: SinkCapability) -> Result<()> {
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
