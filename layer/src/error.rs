use open_arrow_sink_core::Error;

pub(crate) fn failed() -> Error {
    Error::Unsupported("writer failed or was cancelled; abort or drop it".into())
}
