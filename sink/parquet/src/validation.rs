use futures::{TryStreamExt, stream};
use open_arrow_sink_core::Result;
use open_arrow_sink_storage::{Storage, Url};

use crate::error;

// Bound footer readers independently of storage transport or directory size.
const APPEND_SCHEMA_CONCURRENCY: usize = 32;

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
    let metadata = storage.stat_optional(&exact).await?;
    if metadata.as_ref().is_some_and(|metadata| !metadata.is_dir()) {
        return Err(error::not_directory());
    }
    let files = storage.list(directory).await?;
    if metadata.is_none() && files.is_empty() && !storage.exists(directory).await? {
        return Err(error::missing_directory());
    }
    // A transport budget does not limit filesystem reads or the number of
    // live footer readers. Keep schema validation bounded for large directories.
    stream::iter(files.into_iter().map(Ok))
        .try_for_each_concurrent(APPEND_SCHEMA_CONCURRENCY, |file| async move {
            if file
                .path()
                .strip_prefix(directory.path())
                .is_some_and(|relative| !relative.contains('/') && relative.ends_with(".parquet"))
            {
                let actual = crate::reader::schema(storage.clone(), file).await?;
                if &actual != schema {
                    return Err(open_arrow_sink_core::Error::SchemaMismatch);
                }
            }
            Ok(())
        })
        .await?;
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

#[cfg(test)]
#[path = "validation_tests.rs"]
mod tests;
