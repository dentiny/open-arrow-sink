use iceberg::arrow::schema_to_arrow_schema;
use iceberg::spec::Transform;
use iceberg::table::Table;
use open_arrow_sink_core::{
    Compression, Error, PartitionOptions, PartitionType, Result, SchemaRef, SinkCapability,
    WriteOptions, ZSTD_COMPRESSION_LEVEL_RANGE, validation,
};

use crate::error;

pub(crate) fn validate_options(
    schema: &SchemaRef,
    options: &WriteOptions,
    capability: SinkCapability,
) -> Result<()> {
    if options.io_metrics.is_some() {
        return Err(Error::Unsupported(
            "Iceberg session io_metrics: configure the catalog's OpenDalStorageFactory with Storage::with_io_metrics instead".into(),
        ));
    }
    validation::validate_options(schema, options, capability)?;
    if let Some(Compression::Zstd { level: Some(level) }) = options.compression.global
        && !ZSTD_COMPRESSION_LEVEL_RANGE.contains(&level)
    {
        return Err(Error::InvalidConfig(format!(
            "Iceberg Zstd compression level must be between {} and {}: {level}",
            ZSTD_COMPRESSION_LEVEL_RANGE.start(),
            ZSTD_COMPRESSION_LEVEL_RANGE.end()
        )));
    }
    Ok(())
}

pub(crate) fn validate_table(
    table: &Table,
    schema: &SchemaRef,
    partition: Option<PartitionOptions>,
) -> Result<()> {
    if table.readonly() {
        return Err(Error::Unsupported("read-only Iceberg table".into()));
    }
    let spec = table.metadata().default_partition_spec();
    if !spec.fields().is_empty() && partition.is_none() {
        return Err(Error::InvalidConfig(
            "partitioned tables require WriteOptions.partition".into(),
        ));
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
                    return Err(Error::Unsupported(format!(
                        "Iceberg partition transform {transform:?} on field {}",
                        field.name
                    )));
                }
            };
            if category != partition.partition_type {
                return Err(Error::InvalidConfig(format!(
                    "requested {:?} partitioning, but field {} uses {:?}",
                    partition.partition_type, field.name, field.transform
                )));
            }
        }
    }
    let table_schema =
        schema_to_arrow_schema(table.metadata().current_schema()).map_err(Error::backend)?;
    if schema.as_ref() != &table_schema {
        return Err(Error::SchemaMismatch);
    }
    Ok(())
}

/// Parse native SDK locations before validating them against the storage scope.
pub(crate) fn storage_location(path: &str) -> iceberg::Result<open_arrow_sink_storage::Url> {
    if path.starts_with('/') {
        open_arrow_sink_storage::Url::from_file_path(path).map_err(|_| {
            iceberg::Error::new(
                iceberg::ErrorKind::DataInvalid,
                "invalid filesystem location",
            )
        })
    } else {
        open_arrow_sink_storage::Url::parse(path).map_err(error::storage_backend)
    }
}
