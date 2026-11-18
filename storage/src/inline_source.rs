//! Inline sources decoded within the download group's reserved payload budget.
use crate::{DownloadContext, DownloadStream, error};
use data_url::{DataUrl, DataUrlError, forgiving_base64::DecodeError};
use open_arrow_sink_core::{Error, Result};

pub(crate) fn open(source: &str, context: &DownloadContext) -> Result<Option<DownloadStream>> {
    // Parse the borrowed input directly, avoiding a URL-sized allocation.
    let url = match DataUrl::process(source) {
        Ok(url) => url,
        Err(DataUrlError::NotADataUrl) => return Ok(None),
        Err(error) => return Err(Error::InvalidInput(error.to_string())),
    };
    let limit = context.remaining_payload_bytes();
    let mut body = Vec::new();
    url.decode(|bytes| {
        if bytes.len() > limit - body.len() {
            return Err(error::download_too_large(limit));
        }
        if bytes.len() > body.capacity() - body.len() {
            let capacity = body
                .capacity()
                .saturating_mul(2)
                .max(context.chunk_bytes())
                .max(body.len() + bytes.len())
                .min(limit);
            body.try_reserve_exact(capacity - body.len())
                .map_err(Error::backend)?;
        }
        body.extend_from_slice(bytes);
        Ok(())
    })
    .map_err(|error| match error {
        DecodeError::WriteError(error) => error,
        DecodeError::InvalidBase64(_) => Error::InvalidInput("invalid data URL base64 body".into()),
    })?;
    // This buffer fits in the group's payload/copy headroom. Splitting Bytes
    // shares its allocation rather than making another copy for each chunk.
    let mut body = bytes::Bytes::from(body);
    let chunk_bytes = context.chunk_bytes();
    Ok(Some(Box::pin(async_stream::try_stream! {
        while !body.is_empty() {
            let size = chunk_bytes.min(body.len());
            yield body.split_to(size);
        }
    })))
}
