use std::{borrow::Cow, ops::Range};

use open_arrow_sink_core::Result;
use percent_encoding::percent_decode_str;
use url::{Position, Url};

use crate::error;

pub(crate) fn validate_url(url: &Url) -> Result<()> {
    if url.cannot_be_a_base()
        || url.query().is_some()
        || url.fragment().is_some()
        || !url.username().is_empty()
        || url.password().is_some()
    {
        return Err(error::invalid_url());
    }
    Ok(())
}

pub(crate) fn validate_file_url(url: &Url) -> Result<()> {
    url.to_file_path()
        .map(|_| ())
        .map_err(|_| error::invalid_file_url())
}

pub(crate) fn decoded_path(url: &Url) -> Result<Cow<'_, str>> {
    let decoded = percent_decode_str(url.path())
        .decode_utf8()
        .map_err(|_| error::invalid_utf8_path())?;
    validate_path(decoded.trim_start_matches('/'))?;
    Ok(decoded)
}

pub(crate) fn relative_path(base_url: &Url, base: &str, url: &Url) -> Result<String> {
    validate_url(url)?;
    if url[..Position::BeforePath] != base_url[..Position::BeforePath] {
        return Err(error::endpoint_mismatch());
    }
    let path = decoded_path(url)?;
    if path.trim_end_matches('/') == base.trim_end_matches('/') {
        return Ok(String::new());
    }
    path.strip_prefix(base)
        .map(str::to_owned)
        .ok_or_else(error::outside_root)
}

pub(crate) fn validate_path(path: &str) -> Result<()> {
    if path.contains(['\\', '\0'])
        || path
            .trim_end_matches('/')
            .split('/')
            .any(|part| part == "." || part == "..")
    {
        return Err(error::invalid_path());
    }
    Ok(())
}

pub(crate) fn validate_nonroot_path(path: &str) -> Result<()> {
    if path.trim_matches('/').is_empty() {
        return Err(error::empty_path());
    }
    Ok(())
}

pub(crate) fn validate_range(range: &Range<u64>) -> Result<()> {
    if range.start > range.end {
        return Err(error::invalid_read_range());
    }
    Ok(())
}

pub(crate) fn validate_writer_active(active: bool) -> Result<()> {
    if !active {
        return Err(error::writer_inactive());
    }
    Ok(())
}

impl crate::StorageConfig {
    /// Validate policies without constructing a backend or performing I/O.
    pub fn validate(&self) -> Result<()> {
        if let Some(retry) = &self.retry {
            if !retry.factor.is_finite() || retry.factor < 1.0 {
                return Err(error::invalid_retry_factor());
            }
            if retry.min_delay > retry.max_delay {
                return Err(error::invalid_retry_delays());
            }
        }
        if let Some(timeout) = &self.timeout
            && (timeout.operation_timeout.is_zero() || timeout.io_timeout.is_zero())
        {
            return Err(error::invalid_timeouts());
        }
        Ok(())
    }
}

pub(crate) fn validate_http_concurrency(limit: usize) -> Result<()> {
    if limit == 0 || limit > tokio::sync::Semaphore::MAX_PERMITS {
        return Err(error::invalid_http_concurrency());
    }
    Ok(())
}

#[cfg(feature = "http")]
pub(crate) fn validate_download_limit(limit: std::num::NonZeroUsize) -> Result<()> {
    if limit.get() > tokio::sync::Semaphore::MAX_PERMITS {
        return Err(error::download_limit());
    }
    Ok(())
}

#[cfg(feature = "http")]
pub(crate) fn validate_download_url(source: &str) -> Result<http::Uri> {
    let mut url = url::Url::parse(source).map_err(|_| error::invalid_download_url())?;
    if !matches!(url.scheme(), "http" | "https") {
        return Err(error::download_scheme());
    }
    url.set_fragment(None);
    url.as_str()
        .parse()
        .map_err(|_| error::invalid_download_url())
}
