use iceberg::spec::Transform;
use open_arrow_sink_core::{Error, PartitionType};

pub(crate) fn backend(error: iceberg::Error) -> Error {
    Error::Backend(Box::new(error))
}

pub(crate) fn invalid_zstd_level(level: i32) -> Error {
    Error::Unsupported(format!(
        "Iceberg Zstd compression level must be between 1 and 22: {level}"
    ))
}

pub(crate) fn read_only_table() -> Error {
    Error::Unsupported("read-only Iceberg table".into())
}

pub(crate) fn partition_required() -> Error {
    Error::Unsupported("partitioned tables require WriteOptions.partition".into())
}

pub(crate) fn unsupported_partition_transform(transform: Transform, field: &str) -> Error {
    Error::Unsupported(format!(
        "Iceberg partition transform {transform:?} on field {field}"
    ))
}

pub(crate) fn partition_mismatch(
    requested: PartitionType,
    field: &str,
    transform: Transform,
) -> Error {
    Error::Unsupported(format!(
        "requested {requested:?} partitioning, but field {field} uses {transform:?}"
    ))
}

pub(crate) fn schema_mismatch() -> Error {
    Error::SchemaMismatch
}

pub(crate) fn failed_write() -> Error {
    Error::Unsupported("abort or drop the failed writer".into())
}

pub(crate) fn failed_commit() -> Error {
    Error::Unsupported("cannot commit a failed writer".into())
}

pub(crate) fn partition_writer_closed() -> iceberg::Error {
    iceberg::Error::new(iceberg::ErrorKind::Unexpected, "partition writer is closed")
}
