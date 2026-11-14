use std::{io, path::PathBuf};

use open_arrow_sink_core::{
    Ordering, Result, SchemaRef, Sink, SinkCapability, WriteOptions, Writer, async_trait,
    validation,
};

use crate::{error::backend, writer::LanceWriter};

/// Creates a new local dataset directory. Existing paths are always rejected.
/// Only unordered writes are supported; dataset scan order is not guaranteed.
#[derive(Debug, Clone, Copy, Default)]
pub struct LanceDatasetSink;

#[async_trait]
impl Sink for LanceDatasetSink {
    type Request = PathBuf;

    fn capability(&self) -> SinkCapability {
        SinkCapability {
            ordering: Ordering::Unordered,
        }
    }

    async fn open(
        &self,
        path: PathBuf,
        schema: SchemaRef,
        options: WriteOptions,
    ) -> Result<Box<dyn Writer>> {
        validation::validate_options(&options, self.capability())?;
        // Reserve a fresh destination. The caller must keep exclusive ownership of
        // this directory until commit/abort; no existing dataset is overwritten.
        // TODO: Use OpenDAL to support remote storage beyond local paths.
        let path = std::path::absolute(path).map_err(backend)?;
        let uri = path
            .to_str()
            .ok_or_else(|| {
                backend(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "Lance requires a UTF-8 path",
                ))
            })?
            .to_owned();
        tokio::fs::create_dir(&path).await.map_err(backend)?;
        Ok(Box::new(LanceWriter::new(
            schema,
            path,
            uri,
            options.target_file_size,
            options.cleanup_on_error,
        )))
    }
}
