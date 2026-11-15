use std::collections::HashSet;

use arrow_schema::{DataType, Schema};
use open_arrow_sink_core::{Error, Result};

use crate::error::failure;

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
    // HashSet iteration order must not change the order of downloads.
    indices.sort_unstable();
    Ok(indices)
}

pub(crate) fn validate_blob_url(url: &str) -> Result<reqwest::Url> {
    let url = reqwest::Url::parse(url).map_err(|_| failure("invalid blob URL"))?;
    if !matches!(url.scheme(), "http" | "https") {
        return Err(failure("blob URL must use HTTP or HTTPS"));
    }
    Ok(url)
}

pub(crate) fn validate_blob_response(response: &reqwest::Response, limit: usize) -> Result<()> {
    if !response.status().is_success() {
        return Err(failure(format!(
            "unexpected HTTP status: {}",
            response.status()
        )));
    }
    if response.content_length().is_some_and(|n| n > limit as u64) {
        return Err(failure("blob exceeds max_blob_bytes"));
    }
    Ok(())
}

pub(crate) fn validate_blob_chunk(buffered: usize, chunk: usize, limit: usize) -> Result<()> {
    if chunk > limit - buffered {
        return Err(failure("blob exceeds max_blob_bytes"));
    }
    Ok(())
}
