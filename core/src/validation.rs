//! Mandatory shared contracts, separate from optional business validation layers.
use crate::{Error, Ordering, RecordBatch, Result, SinkCapability, WriteOptions};
use arrow_schema::Schema;

/// Check declared capabilities before opening a destination or producing side effects.
pub fn validate_options(options: &WriteOptions, capability: SinkCapability) -> Result<()> {
    if let Some(partition) = options.partition
        && !capability
            .supported_partitions
            .contains(&partition.partition_type)
    {
        return Err(Error::Unsupported(format!(
            "partitioned writes with {:?}",
            partition.partition_type
        )));
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
