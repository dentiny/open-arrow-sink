use iceberg::spec::Transform;
use iceberg::table::Table;
use open_arrow_sink_core::{Error, PartitionOptions, PartitionType, Result};

pub(crate) fn validate_table(table: &Table, partition: Option<PartitionOptions>) -> Result<()> {
    if table.readonly() {
        return Err(Error::Unsupported("read-only Iceberg table".into()));
    }
    let spec = table.metadata().default_partition_spec();
    if !spec.fields().is_empty() && partition.is_none() {
        return Err(Error::Unsupported(
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
                return Err(Error::Unsupported(format!(
                    "requested {:?} partitioning, but field {} uses {:?}",
                    partition.partition_type, field.name, field.transform
                )));
            }
        }
    }
    Ok(())
}
