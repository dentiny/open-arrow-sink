use std::{borrow::Cow, ops::Range};

use open_arrow_sink_core::{Error, Result};
use percent_encoding::percent_decode_str;
use url::{Position, Url};

pub(crate) fn validate_url(url: &Url) -> Result<()> {
    if url.cannot_be_a_base()
        || url.query().is_some()
        || url.fragment().is_some()
        || !url.username().is_empty()
        || url.password().is_some()
    {
        return Err(Error::InvalidInput(
            "storage URLs must be hierarchical and contain no query, fragment, or credentials"
                .into(),
        ));
    }
    Ok(())
}

pub(crate) fn validate_file_url(url: &Url) -> Result<()> {
    url.to_file_path()
        .map(|_| ())
        .map_err(|_| Error::InvalidInput("invalid file URL".into()))
}

pub(crate) fn decoded_path(url: &Url) -> Result<Cow<'_, str>> {
    let decoded = percent_decode_str(url.path())
        .decode_utf8()
        .map_err(|_| Error::InvalidInput("URL path must be UTF-8".into()))?;
    validate_path(decoded.trim_start_matches('/'))?;
    Ok(decoded)
}

pub(crate) fn relative_path(base_url: &Url, base: &str, url: &Url) -> Result<String> {
    validate_url(url)?;
    if url[..Position::BeforePath] != base_url[..Position::BeforePath] {
        return Err(Error::InvalidInput(
            "URL does not belong to this storage endpoint".into(),
        ));
    }
    let path = decoded_path(url)?;
    if path.trim_end_matches('/') == base.trim_end_matches('/') {
        return Ok(String::new());
    }
    path.strip_prefix(base)
        .map(str::to_owned)
        .ok_or_else(|| Error::InvalidInput("URL is outside the storage root".into()))
}

pub(crate) fn validate_path(path: &str) -> Result<()> {
    if path.contains(['\\', '\0'])
        || path
            .trim_end_matches('/')
            .split('/')
            .any(|part| part == "." || part == "..")
    {
        return Err(Error::InvalidInput("invalid storage path".into()));
    }
    Ok(())
}

pub(crate) fn validate_nonroot_path(path: &str) -> Result<()> {
    if path.trim_matches('/').is_empty() {
        return Err(Error::InvalidInput(
            "operation requires a nonempty path".into(),
        ));
    }
    Ok(())
}

pub(crate) fn validate_range(range: &Range<u64>) -> Result<()> {
    if range.start > range.end {
        return Err(Error::InvalidInput("invalid read range".into()));
    }
    Ok(())
}

impl crate::StorageConfig {
    /// Validate policies without constructing a backend or performing I/O.
    pub fn validate(&self) -> Result<()> {
        if let Some(retry) = &self.retry {
            if !retry.factor.is_finite() || retry.factor < 1.0 {
                return Err(Error::InvalidConfig(
                    "retry factor must be finite and at least 1".into(),
                ));
            }
            if retry.min_delay > retry.max_delay {
                return Err(Error::InvalidConfig(
                    "retry min_delay must not exceed max_delay".into(),
                ));
            }
        }
        if let Some(timeout) = &self.timeout
            && (timeout.operation_timeout.is_zero() || timeout.io_timeout.is_zero())
        {
            return Err(Error::InvalidConfig(
                "storage timeouts must be positive".into(),
            ));
        }
        Ok(())
    }
}

pub(crate) fn validate_http_concurrency(limit: usize) -> Result<()> {
    if limit == 0 || limit > tokio::sync::Semaphore::MAX_PERMITS {
        return Err(Error::InvalidConfig(
            "HTTP concurrency must be positive and not exceed Semaphore::MAX_PERMITS".into(),
        ));
    }
    Ok(())
}
