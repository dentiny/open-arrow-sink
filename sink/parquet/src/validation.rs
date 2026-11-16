use futures::{TryStreamExt, stream};
use open_arrow_sink_core::{Error, Result};
use open_arrow_sink_storage::{Storage, Url};

// Bound footer readers independently of storage transport or directory size.
const APPEND_SCHEMA_CONCURRENCY: usize = 32;

pub(crate) async fn validate_append_directory(
    storage: &Storage,
    directory: &Url,
    schema: &open_arrow_sink_core::SchemaRef,
) -> Result<()> {
    let mut exact = directory.clone();
    exact.path_segments_mut().unwrap().pop_if_empty();
    let metadata = storage.stat_optional(&exact).await?;
    if metadata.as_ref().is_some_and(|metadata| !metadata.is_dir()) {
        return Err(Error::InvalidInput(
            "destination must be a directory".into(),
        ));
    }
    let files = storage.list(directory).await?;
    if metadata.is_none() && files.is_empty() && !storage.exists(directory).await? {
        return Err(Error::NotFound(
            "destination directory does not exist".into(),
        ));
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
                    return Err(Error::SchemaMismatch);
                }
            }
            Ok(())
        })
        .await?;
    Ok(())
}
