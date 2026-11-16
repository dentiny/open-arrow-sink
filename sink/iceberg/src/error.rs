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
