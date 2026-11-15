use open_arrow_sink_core::Result;
use open_arrow_sink_storage::{Storage, Url};

use crate::error;

pub(crate) fn validate_destination_path(storage: &Storage, directory: &Url) -> Result<()> {
    if storage.path(directory)?.trim_matches('/').is_empty() {
        return Err(error::empty_destination());
    }
    Ok(())
}

/// Preflight only; the subsequent conditional marker creation arbitrates races.
pub(crate) async fn validate_fresh_destination(storage: &Storage, directory: &Url) -> Result<()> {
    let mut exact = directory.clone();
    exact.path_segments_mut().unwrap().pop_if_empty();
    if storage.exists(&exact).await?
        || storage.exists(directory).await?
        || !storage.list(directory).await?.is_empty()
    {
        return Err(error::destination_exists());
    }
    Ok(())
}

pub(crate) fn validate_writer_active(active: bool) -> Result<()> {
    if !active {
        return Err(error::closed());
    }
    Ok(())
}
