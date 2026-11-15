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

pub(crate) async fn validate_append_directory(
    storage: &Storage,
    directory: &Url,
    schema: &open_arrow_sink_core::SchemaRef,
) -> Result<()> {
    let mut exact = directory.clone();
    exact.path_segments_mut().unwrap().pop_if_empty();
    let exact_exists = storage.exists(&exact).await?;
    if exact_exists && !storage.stat(&exact).await?.is_dir() {
        return Err(error::not_directory());
    }
    let files = storage.list(directory).await?;
    if !exact_exists && files.is_empty() && !storage.exists(directory).await? {
        return Err(error::missing_directory());
    }
    for file in files {
        if file
            .path()
            .strip_prefix(directory.path())
            .is_some_and(|relative| !relative.contains('/') && relative.ends_with(".parquet"))
        {
            let actual = crate::reader::schema(storage.clone(), file).await?;
            if &actual != schema {
                return Err(error::schema_mismatch());
            }
        }
    }
    Ok(())
}

pub(crate) async fn validate_overwrite_directory(storage: &Storage, directory: &Url) -> Result<()> {
    if storage.operator().info().scheme() != "fs" {
        return Err(error::overwrite_unsupported());
    }
    // OpenDAL stat follows symlinks. This local-only guard preserves main's
    // no-follow check before a whole directory is renamed and replaced.
    let physical = std::path::PathBuf::from(storage.operator().info().root())
        .join(storage.path(directory)?.trim_end_matches('/'));
    match tokio::fs::symlink_metadata(physical).await {
        Ok(metadata) if !metadata.is_dir() => Err(error::not_directory()),
        Ok(_) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(crate::error::backend(error)),
    }
}
