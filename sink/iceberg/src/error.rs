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

pub(crate) fn unsupported_table() -> Error {
    Error::Unsupported("only writable, unpartitioned tables are supported".into())
}
