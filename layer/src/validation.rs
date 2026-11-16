use open_arrow_sink_core::{Result, SchemaRef, WriteSession};

use crate::error;

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
        return Err(error::invalid_session());
    }
    Ok(())
}
