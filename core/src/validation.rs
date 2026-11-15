//! Mandatory shared contracts, separate from optional business validation layers.
use std::collections::HashSet;

use crate::{
    CompressionCapability, CompressionOptions, Error, IndexSpec, IndexType, Ordering,
    PartitionOptions, PartitionType, RecordBatch, Result, SinkCapability, WriteOptions,
};
use arrow_schema::Schema;

/// Check declared capabilities before opening a destination or producing side effects.
/// Backends must additionally validate codec-specific parameters, such as Zstd levels.
pub fn validate_options(options: &WriteOptions, capability: SinkCapability) -> Result<()> {
    validate_compression_options(&options.compression, capability.compression)?;
    validate_partition_options(options.partition, &capability.supported_partitions)?;
    validate_index_options(&options.indexes, capability.supported_indexes)?;
    validate_target_file_size_options(options.target_file_size)?;
    validate_ordering_options(options.ordering, capability.ordering)?;
    Ok(())
}

/// Check compression scopes and codecs. Backends additionally validate levels.
pub fn validate_compression_options(
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
        return Err(Error::Unsupported(format!(
            "partitioned writes with {:?}",
            partition.partition_type
        )));
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
            return Err(Error::Unsupported(format!(
                "{:?} index construction on column {}",
                index.index_type, index.column
            )));
        }
    }
    Ok(())
}

/// Check that an explicit target file size is positive.
pub fn validate_target_file_size_options(target_file_size: Option<usize>) -> Result<()> {
    if target_file_size == Some(0) {
        return Err(Error::Unsupported(
            "target_file_size must be greater than zero".into(),
        ));
    }
    Ok(())
}

/// Check that the sink can provide the requested input-order guarantee.
pub fn validate_ordering_options(ordering: Ordering, supported_ordering: Ordering) -> Result<()> {
    if ordering == Ordering::PreserveInput && supported_ordering != Ordering::PreserveInput {
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
pub fn validate_compression(schema: &Schema, compression: &CompressionOptions) -> Result<()> {
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
