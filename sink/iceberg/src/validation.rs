use iceberg::arrow::schema_to_arrow_schema;
use iceberg::spec::Transform;
use iceberg::table::Table;
use open_arrow_sink_core::{
    Compression, PartitionOptions, PartitionType, Result, SchemaRef, SinkCapability, WriteOptions,
    ZSTD_COMPRESSION_LEVEL_RANGE, validation,
};

use crate::error;

pub(crate) fn validate_options(
    schema: &SchemaRef,
    options: &WriteOptions,
    capability: SinkCapability,
) -> Result<()> {
    validation::validate_options(schema, options, capability)?;
    if let Some(Compression::Zstd { level: Some(level) }) = options.compression.global
        && !ZSTD_COMPRESSION_LEVEL_RANGE.contains(&level)
    {
        return Err(error::invalid_zstd_level(level));
    }
    Ok(())
}

pub(crate) fn validate_table(
    table: &Table,
    schema: &SchemaRef,
    partition: Option<PartitionOptions>,
) -> Result<()> {
    if table.readonly() {
        return Err(error::read_only_table());
    }
    let spec = table.metadata().default_partition_spec();
    if !spec.fields().is_empty() && partition.is_none() {
        return Err(error::partition_required());
    }
    if let Some(partition) = partition {
        for field in spec.fields() {
            let category = match field.transform {
                Transform::Identity => PartitionType::Identity,
                Transform::Year => PartitionType::Year,
                Transform::Month => PartitionType::Month,
                Transform::Day => PartitionType::Day,
                Transform::Hour => PartitionType::Hour,
                // The SDK uses a signed 32-bit divisor when hashing buckets.
                Transform::Bucket(n) if n > 0 && n <= i32::MAX as u32 => PartitionType::Bucket,
                Transform::Truncate(width) if width > 0 => PartitionType::Truncate,
                Transform::Void => PartitionType::Void,
                transform => {
                    return Err(error::unsupported_partition_transform(
                        transform,
                        &field.name,
                    ));
                }
            };
            if category != partition.partition_type {
                return Err(error::partition_mismatch(
                    partition.partition_type,
                    &field.name,
                    field.transform,
                ));
            }
        }
    }
    let table_schema =
        schema_to_arrow_schema(table.metadata().current_schema()).map_err(error::backend)?;
    if schema.as_ref() != &table_schema {
        return Err(error::schema_mismatch());
    }
    Ok(())
}

pub(crate) fn validate_write_state(failed: bool) -> Result<()> {
    if failed {
        return Err(error::failed_write());
    }
    Ok(())
}

pub(crate) fn validate_commit_state(failed: bool) -> Result<()> {
    if failed {
        return Err(error::failed_commit());
    }
    Ok(())
}

/// Parse native SDK locations before validating them against the storage scope.
pub(crate) fn storage_location(path: &str) -> iceberg::Result<open_arrow_sink_storage::Url> {
    if path.starts_with('/') {
        open_arrow_sink_storage::Url::from_file_path(path)
            .map_err(|_| error::invalid_storage_location())
    } else {
        open_arrow_sink_storage::Url::parse(path).map_err(error::storage_backend)
    }
}
