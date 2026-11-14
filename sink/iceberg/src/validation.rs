use iceberg::spec::Transform;
use iceberg::table::Table;
use open_arrow_sink_core::{Error, PartitionOptions, Result};

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
    if spec
        .fields()
        .iter()
        .any(|field| field.transform != Transform::Identity)
    {
        return Err(Error::Unsupported(
            "only identity partition transforms are supported".into(),
        ));
    }
    Ok(())
}
