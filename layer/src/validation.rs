//! Download pool configuration and source URL validation.
use std::num::NonZeroUsize;

use http::Uri;
use open_arrow_sink_core::{Error, Result};
use tokio::sync::Semaphore;

use crate::error::failure;

pub(crate) fn validate_download_limit(limit: NonZeroUsize) -> Result<()> {
    if limit.get() > Semaphore::MAX_PERMITS {
        return Err(Error::Unsupported(
            "download pool limits exceed semaphore capacity".into(),
        ));
    }
    Ok(())
}

pub(crate) fn validate_download_url(source: &str) -> Result<Uri> {
    let mut url = url::Url::parse(source).map_err(|_| failure("invalid blob URL"))?;
    if !matches!(url.scheme(), "http" | "https") {
        return Err(failure("blob URL must use HTTP or HTTPS"));
    }
    url.set_fragment(None);
    url.as_str()
        .parse()
        .map_err(|_| failure("invalid blob URL"))
}
