//! Mandatory shared contracts, separate from optional business validation layers.
use std::collections::HashSet;

use crate::error::{
    duplicate_index, invalid_target_file_size, missing_index_column, preserve_input_unsupported,
    unsupported_index, unsupported_partition,
};
use crate::{
    CompressionCapability, CompressionOptions, Error, IndexSpec, IndexType, Ordering,
    PartitionOptions, PartitionType, RecordBatch, Result, SinkCapability, WriteOptions,
};
use arrow_schema::Schema;

/// Check declared capabilities and compression column names before destination I/O.
/// Backends must additionally validate codec-specific parameters, such as Zstd levels.
pub fn validate_options(
    schema: &Schema,
    options: &WriteOptions,
    capability: SinkCapability,
) -> Result<()> {
    validate_mode_options(options.mode, capability.supported_modes)?;
    validate_storage_version_options(
        options.storage_version.as_deref(),
        capability.supports_storage_version,
    )?;
    validate_compression_options(schema, &options.compression, capability.compression)?;
    validate_partition_options(options.partition, &capability.supported_partitions)?;
    validate_index_options(&options.indexes, capability.supported_indexes)?;
    validate_target_file_size_options(options.target_file_size)?;
    validate_ordering_options(options.ordering, capability.ordering)?;
    Ok(())
}

/// Reject unsupported modes, including the default, before destination I/O.
pub fn validate_mode_options(
    mode: crate::WriteMode,
    supported_modes: &[crate::WriteMode],
) -> Result<()> {
    if !supported_modes.contains(&mode) {
        return Err(Error::Unsupported(format!("write mode: {mode:?}")));
    }
    Ok(())
}

/// Check compression scopes, codecs, and exact top-level column names.
/// Backends additionally validate levels.
pub fn validate_compression_options(
    schema: &Schema,
    options: &CompressionOptions,
    capability: CompressionCapability,
) -> Result<()> {
    if let Some(compression) = options.global
        && !capability.global.contains(&compression.codec())
    {
        return Err(Error::Unsupported(format!(
            "global compression: {compression:?}"
        )));
    }
    for (column, compression) in &options.columns {
        if !capability.per_column.contains(&compression.codec()) {
            return Err(Error::Unsupported(format!(
                "compression {compression:?} on column {column}"
            )));
        }
    }
    for column in options.columns.keys() {
        if schema
            .fields()
            .iter()
            .filter(|field| field.name() == column)
            .count()
            != 1
        {
            return Err(Error::Unsupported(format!(
                "compression column must identify one top-level field: {column}"
            )));
        }
    }
    Ok(())
}

/// Check that the sink supports the requested partition transform.
pub fn validate_partition_options(
    options: Option<PartitionOptions>,
    supported_partitions: &[PartitionType],
) -> Result<()> {
    if let Some(partition) = options
        && !supported_partitions.contains(&partition.partition_type)
    {
        return Err(unsupported_partition(partition.partition_type));
    }
    Ok(())
}

/// Check index types. Schema and column-type checks are performed separately.
pub fn validate_index_options(
    indexes: &[IndexSpec],
    supported_indexes: &[IndexType],
) -> Result<()> {
    for index in indexes {
        if !supported_indexes.contains(&index.index_type) {
            return Err(unsupported_index(index));
        }
    }
    Ok(())
}

/// Check that an explicit target file size is positive.
pub fn validate_target_file_size_options(target_file_size: Option<usize>) -> Result<()> {
    if target_file_size == Some(0) {
        return Err(invalid_target_file_size());
    }
    Ok(())
}

/// Check that the sink can provide the requested input-order guarantee.
pub fn validate_ordering_options(ordering: Ordering, supported_ordering: Ordering) -> Result<()> {
    if ordering == Ordering::PreserveInput && supported_ordering != Ordering::PreserveInput {
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

/// Reject explicit storage versions when the backend does not support them.
/// Supporting backends additionally validate their version syntax.
pub fn validate_storage_version_options(
    storage_version: Option<&str>,
    supports_storage_version: bool,
) -> Result<()> {
    if storage_version.is_some() && !supports_storage_version {
        return Err(Error::Unsupported("storage version".into()));
    }
    Ok(())
}
