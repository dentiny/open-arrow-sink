use iceberg::spec::Transform;
use open_arrow_sink_core::{Error, PartitionType, ZSTD_COMPRESSION_LEVEL_RANGE};

pub(crate) fn backend(error: iceberg::Error) -> Error {
    Error::Backend(Box::new(error))
}

pub(crate) fn invalid_zstd_level(level: i32) -> Error {
    Error::Unsupported(format!(
        "Iceberg Zstd compression level must be between {} and {}: {level}",
        ZSTD_COMPRESSION_LEVEL_RANGE.start(),
        ZSTD_COMPRESSION_LEVEL_RANGE.end(),
    ))
}

pub(crate) fn read_only_table() -> Error {
    Error::Unsupported("read-only Iceberg table".into())
}

pub(crate) fn partition_required() -> Error {
    Error::Unsupported("partitioned tables require IcebergTableSink::with_partition".into())
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

pub(crate) fn storage_backend(
    error: impl std::error::Error + Send + Sync + 'static,
) -> iceberg::Error {
    iceberg::Error::new(
        iceberg::ErrorKind::Unexpected,
        "OpenDAL storage operation failed",
    )
    .with_source(error)
}

pub(crate) fn invalid_storage_location() -> iceberg::Error {
    iceberg::Error::new(
        iceberg::ErrorKind::DataInvalid,
        "invalid filesystem location",
    )
}

pub(crate) fn operator_serialization<E: serde::ser::Error>() -> E {
    E::custom("live OpenDAL operators cannot be serialized")
}

pub(crate) fn operator_deserialization<E: serde::de::Error>() -> E {
    E::custom("construct OpenDAL storage with a configured operator")
}
