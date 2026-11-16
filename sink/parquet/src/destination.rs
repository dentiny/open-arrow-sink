use bytes::Bytes;
use open_arrow_sink_core::{Error, Result, SchemaRef, WriteMode};
use open_arrow_sink_storage::{DirectoryState, ObjectPath, Storage, Url};
use parquet::{arrow::AsyncArrowWriter, file::properties::WriterProperties};

use crate::file_writer::FileWriter;

pub(crate) struct Destination {
    pub(crate) storage: Storage,
    pub(crate) path: ObjectPath,
    prefix: String,
    create_empty_file: bool,
}

impl Destination {
    pub(crate) async fn open(
        storage: Storage,
        directory: Url,
        schema: &SchemaRef,
        mode: WriteMode,
    ) -> Result<Self> {
        let directory = storage.path(&directory)?.as_dir();
        if directory.is_root() {
            return Err(Error::InvalidInput(
                "destination must not be the storage root".into(),
            ));
        }
        let prefix = match mode {
            WriteMode::Create => {
                if storage.inspect_directory(&directory).await? != DirectoryState::Missing {
                    return Err(Error::AlreadyExists(storage.url(&directory)?.to_string()));
                }
                "part-".into()
            }
            WriteMode::Append => {
                crate::validation::validate_append_directory(&storage, &directory, schema).await?;
                format!("part-{}-", uuid::Uuid::new_v4())
            }
            WriteMode::Overwrite => {
                return Err(Error::Unsupported(
                    "Parquet directory sink does not support Overwrite".into(),
                ));
            }
        };
        Ok(Self {
            storage,
            path: directory,
            prefix,
            create_empty_file: matches!(mode, WriteMode::Create),
        })
    }

    pub(crate) fn needs_empty_file(&self) -> bool {
        self.create_empty_file
    }

    pub(crate) async fn write_empty_file(
        &self,
        schema: SchemaRef,
        properties: WriterProperties,
    ) -> Result<()> {
        self.open_writer(0, 0, schema, properties)
            .await?
            .finish()
            .await
            .map_err(Error::backend)?;
        Ok(())
    }

    pub(crate) async fn open_writer(
        &self,
        writer: usize,
        sequence: usize,
        schema: SchemaRef,
        properties: WriterProperties,
    ) -> Result<AsyncArrowWriter<FileWriter>> {
        let path = self
            .path
            .join(&format!("{}{writer}-{sequence:05}.parquet", self.prefix))
            .unwrap();
        self.storage.create(&path, Bytes::new()).await?;
        let file = FileWriter(self.storage.writer(&path).await?);
        AsyncArrowWriter::try_new(file, schema, Some(properties)).map_err(Error::backend)
    }
}
