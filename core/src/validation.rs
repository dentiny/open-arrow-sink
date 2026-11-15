//! Mandatory shared contracts, separate from optional business validation layers.
use std::collections::HashSet;

use crate::{Error, IndexSpec, Ordering, RecordBatch, Result, SinkCapability, WriteOptions};
use arrow_schema::Schema;

/// Check declared capabilities before opening a destination or producing side effects.
/// Backends must additionally validate codec-specific parameters, such as Zstd levels.
pub fn validate_options(options: &WriteOptions, capability: SinkCapability) -> Result<()> {
    if let Some(compression) = options.compression.global
        && !capability.compression.global.contains(&compression.codec())
    {
        return Err(Error::Unsupported(format!(
            "global compression: {compression:?}"
        )));
    }
    for (column, compression) in &options.compression.columns {
        if !capability
            .compression
            .per_column
            .contains(&compression.codec())
        {
            return Err(Error::Unsupported(format!(
                "compression {compression:?} on column {column}"
            )));
        }
    }
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

/// Validate exact top-level field names before destination I/O.
pub fn validate_compression(
    schema: &Schema,
    compression: &crate::CompressionOptions,
) -> Result<()> {
    for column in compression.columns.keys() {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        Compression, CompressionCapability, CompressionCodec as Codec, CompressionOptions,
    };

    #[test]
    fn compression_scopes_and_codecs_are_checked_independently() {
        let mut capability = SinkCapability {
            ordering: Ordering::PreserveInput,
            compression: CompressionCapability {
                global: &[Codec::Snappy],
                per_column: &[],
            },
            supported_partitions: vec![],
            supported_indexes: &[],
        };
        let mut options = WriteOptions::default();
        assert!(validate_options(&options, capability.clone()).is_ok());
        options.compression.global = Some(Compression::Snappy);
        assert!(validate_options(&options, capability.clone()).is_ok());
        options.compression.global = Some(Compression::Zstd { level: None });
        assert!(validate_options(&options, capability.clone()).is_err());
        options.compression.global = None;
        options
            .compression
            .columns
            .insert("id".into(), Compression::Snappy);
        assert!(validate_options(&options, capability.clone()).is_err());
        capability.compression = CompressionCapability {
            global: &[],
            per_column: &[Codec::Snappy],
        };
        assert!(validate_options(&options, capability.clone()).is_ok());
        options.compression.global = Some(Compression::Snappy);
        assert!(validate_options(&options, capability).is_err());
    }

    #[test]
    fn zstd_capability_is_independent_of_requested_level() {
        let capability = SinkCapability {
            ordering: Ordering::PreserveInput,
            compression: CompressionCapability {
                global: &[Codec::Zstd],
                per_column: &[Codec::Zstd],
            },
            supported_partitions: vec![],
            supported_indexes: &[],
        };
        // Level ranges are backend-specific, not part of shared codec validation.
        for level in [None, Some(-5), Some(0), Some(1), Some(9), Some(22)] {
            let compression = Compression::Zstd { level };
            let mut options = WriteOptions {
                compression: CompressionOptions {
                    global: Some(compression),
                    columns: [("id".into(), compression)].into(),
                },
                ..Default::default()
            };
            assert!(validate_options(&options, capability.clone()).is_ok());
            options
                .compression
                .columns
                .insert("id".into(), Compression::Gzip);
            assert!(validate_options(&options, capability.clone()).is_err());
        }
        assert_eq!(
            Compression::from(Codec::Zstd),
            Compression::Zstd { level: None }
        );
    }

    #[test]
    fn ambiguous_column_names_are_rejected() {
        let field = arrow_schema::Field::new("id", arrow_schema::DataType::Int64, true);
        let schema = Schema::new(vec![field.clone(), field]);
        let compression = CompressionOptions {
            global: None,
            columns: [("id".into(), Compression::Snappy)].into(),
        };
        assert!(validate_compression(&schema, &compression).is_err());
    }
}
