use open_arrow_sink_core::Error;

pub(crate) fn backend(error: iceberg::Error) -> Error {
    Error::Backend(Box::new(error))
}

pub(crate) fn failed() -> Error {
    Error::Unsupported("abort or drop the failed writer".into())
}

pub(crate) fn failed_commit() -> Error {
    Error::Unsupported("cannot commit a failed writer".into())
}

pub(crate) fn readonly_table() -> Error {
    Error::Unsupported("read-only Iceberg table".into())
}

pub(crate) fn partition_required() -> Error {
    Error::Unsupported("partitioned tables require WriteOptions.partition".into())
}

pub(crate) fn unsupported_transform(transform: iceberg::spec::Transform, field: &str) -> Error {
    Error::Unsupported(format!(
        "Iceberg partition transform {transform:?} on field {field}"
    ))
}

pub(crate) fn partition_mismatch(
    requested: open_arrow_sink_core::PartitionType,
    field: &str,
    transform: iceberg::spec::Transform,
) -> Error {
    Error::Unsupported(format!(
        "requested {requested:?} partitioning, but field {field} uses {transform:?}"
    ))
}

pub(crate) fn partition_closed() -> iceberg::Error {
    iceberg::Error::new(iceberg::ErrorKind::Unexpected, "partition writer is closed")
}
