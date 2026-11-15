use std::collections::HashSet;

use arrow_schema::{DataType, Schema};
use open_arrow_sink_core::{Error, Result};

pub(crate) fn validate_download_config(
    schema: &Schema,
    columns: &HashSet<String>,
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
            return Err(Error::Unsupported(format!(
                "download column must exist and be unique: {name}"
            )));
        }
        let (index, field) = matches[0];
        if !matches!(
            field.data_type(),
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
        ) {
            return Err(Error::Unsupported(format!(
                "download column must be a string: {name}"
            )));
        }
        indices.push(index);
    }
    // Resolve selected columns in schema order.
    indices.sort_unstable();
    Ok(indices)
}
