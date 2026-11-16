use futures::{TryStreamExt, stream};
use open_arrow_sink_core::Result;
use open_arrow_sink_storage::{DirectoryState, ObjectPath, Storage};

use crate::error;

// Bound footer readers independently of storage transport or directory size.
const APPEND_SCHEMA_CONCURRENCY: usize = 32;

pub(crate) async fn validate_append_directory(
    storage: &Storage,
    directory: &ObjectPath,
    schema: &open_arrow_sink_core::SchemaRef,
) -> Result<()> {
    match storage.inspect_directory(directory).await? {
        DirectoryState::Missing => return Err(error::missing_directory()),
        DirectoryState::File => return Err(error::not_directory()),
        DirectoryState::Empty => return Ok(()),
        DirectoryState::NonEmpty => {}
    }
    let files = storage.list(directory).await?;
    // A transport budget does not limit filesystem reads or the number of
    // live footer readers. Keep schema validation bounded for large directories.
    stream::iter(files.into_iter().map(Ok))
        .try_for_each_concurrent(APPEND_SCHEMA_CONCURRENCY, |file| async move {
            if file
                .as_str()
                .strip_prefix(directory.as_str())
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
