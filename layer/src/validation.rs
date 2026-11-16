use open_arrow_sink_core::{Error, Result, SchemaRef, WriteSession};

pub(crate) fn validate_session<T: Send + 'static>(
    session: &WriteSession<T>,
    count: usize,
    schema: &SchemaRef,
) -> Result<()> {
    if session.writers.len() != count
        || session
            .writers
            .iter()
            .any(|writer| writer.schema() != schema)
    {
        return Err(Error::backend(std::io::Error::other(
            "backend returned an invalid parallel writer session",
        )));
    }
    Ok(())
}
