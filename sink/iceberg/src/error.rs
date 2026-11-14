use open_arrow_sink_core::Error;

pub(crate) fn backend(error: iceberg::Error) -> Error {
    Error::Backend(Box::new(error))
}
