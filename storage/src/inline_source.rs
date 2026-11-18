//! Decode inline data URLs into the downloader's ordinary payload stream.
use crate::{DownloadContext, DownloadStream};
use data_url::{DataUrl, DataUrlError};
use open_arrow_sink_core::{Error, Result};

pub(crate) fn open(source: &str, context: &DownloadContext) -> Result<Option<DownloadStream>> {
    let url = match DataUrl::process(source) {
        Ok(url) => url,
        Err(DataUrlError::NotADataUrl) => return Ok(None),
        Err(error) => return Err(Error::InvalidInput(error.to_string())),
    };
    let (body, _) = url
        .decode_to_vec()
        .map_err(|_| Error::InvalidInput("invalid data URL base64 body".into()))?;
    let mut body = bytes::Bytes::from(body);
    let chunk_bytes = context.chunk_bytes();
    Ok(Some(Box::pin(async_stream::try_stream! {
        while !body.is_empty() {
            let size = chunk_bytes.min(body.len());
            yield body.split_to(size);
        }
    })))
}
