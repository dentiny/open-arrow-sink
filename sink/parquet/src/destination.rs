use bytes::Bytes;
use open_arrow_sink_core::{Error, Result, SchemaRef, WriteMode};
use open_arrow_sink_storage::{DirectoryState, ObjectPath, Storage, Url};
use parquet::{arrow::AsyncArrowWriter, file::properties::WriterProperties};

use crate::file_writer::FileWriter;

/// Session-owned parts in a destination directory.
pub(crate) struct Destination {
    storage: Storage,
    directory: ObjectPath,
    prefix: String,
    // Only Create may clean up the empty destination directory.
    // Append shares an existing directory and must preserve it.
    owns_directory: bool,
    cleanup_on_error: bool,
}
impl Destination {
    pub(crate) async fn open(
        storage: Storage,
        directory: Url,
        schema: &SchemaRef,
        mode: WriteMode,
        cleanup_on_error: bool,
    ) -> Result<Self> {
        let directory = storage.resolve(&directory)?.as_dir();
        if directory.is_root() {
            return Err(Error::InvalidInput(
                "destination must not be the storage root".into(),
            ));
        }
        if mode == WriteMode::Create
            && storage.inspect_directory(&directory).await? != DirectoryState::Missing
        {
            return Err(Error::AlreadyExists(
                storage.url(directory.as_str())?.to_string(),
            ));
        }
        let (prefix, owns_directory) = match mode {
            WriteMode::Create => ("part-".into(), true),
            WriteMode::Append => {
                crate::validation::validate_append_directory(&storage, &directory, schema).await?;
                (format!("part-{}-", uuid::Uuid::new_v4()), false)
            }
            WriteMode::Overwrite => {
                return Err(Error::Unsupported(
                    "Parquet directory sink does not support Overwrite".into(),
                ));
            }
        };
        Ok(Self {
            storage,
            directory,
            prefix,
            owns_directory,
            cleanup_on_error,
        })
    }
    fn child(&self, name: &str) -> ObjectPath {
        self.directory.join(name).unwrap()
    }
    fn file(&self, index: usize) -> ObjectPath {
        self.child(&format!("{}{index:05}.parquet", self.prefix))
    }
    pub(crate) async fn open_writer(
        &self,
        index: usize,
        schema: SchemaRef,
        properties: WriterProperties,
    ) -> Result<AsyncArrowWriter<FileWriter>> {
        let path = self.file(index);
        self.storage.create(&path, Bytes::new()).await?;
        let result = async {
            let file = FileWriter(self.storage.writer(&path).await?);
            AsyncArrowWriter::try_new(file, schema, Some(properties)).map_err(Error::backend)
        }
        .await;
        if result.is_err() && self.cleanup_on_error {
            let _ = self.storage.delete(&path).await;
        }
        result
    }
    pub(crate) async fn remove_file(&self, index: usize) -> Result<()> {
        self.storage.delete(&self.file(index)).await
    }
    pub(crate) async fn remove_empty_directory(&self) -> Result<()> {
        if !self.owns_directory {
            return Ok(());
        }
        self.storage.remove_empty_directory(&self.directory).await
    }
}
