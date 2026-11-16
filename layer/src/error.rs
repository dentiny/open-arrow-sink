use open_arrow_sink_core::Error;

pub(crate) fn invalid_session() -> Error {
    Error::Unsupported("backend returned an invalid parallel writer session".into())
}
