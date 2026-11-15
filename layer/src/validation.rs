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

pub(crate) fn validate_writer_state(failed: bool) -> Result<()> {
    if failed {
        return Err(error::unsupported());
    }
    Ok(())
}

pub(crate) fn validate_download_config(
    schema: &arrow_schema::Schema,
    columns: &std::collections::HashSet<String>,
) -> Result<Vec<usize>> {
    let mut indices = Vec::new();
    for name in columns {
        let matches: Vec<_> = schema
            .fields()
            .iter()
            .enumerate()
            .filter(|(_, f)| f.name() == name)
            .collect();
        if matches.len() != 1 {
            return Err(error::download_column_unique(name));
        }
        let (index, field) = matches[0];
        if !matches!(
            field.data_type(),
            arrow_schema::DataType::Utf8
                | arrow_schema::DataType::LargeUtf8
                | arrow_schema::DataType::Utf8View
        ) {
            return Err(error::download_column_type(name));
        }
        indices.push(index);
    }
    // Resolve selected columns in schema order.
    indices.sort_unstable();
    Ok(indices)
}

pub(crate) fn validate_download_support(selected: bool, supported: bool) -> Result<()> {
    if selected && !supported {
        return Err(error::streaming_blobs_required());
    }
    Ok(())
}
pub(crate) fn validate_download_writer(failed: bool) -> Result<()> {
    if failed {
        return Err(error::download_failed());
    }
    Ok(())
}
