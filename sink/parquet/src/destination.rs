use bytes::Bytes;
use open_arrow_sink_core::{Error, Result, SchemaRef, WriteMode};
use open_arrow_sink_storage::{DirectoryState, ObjectPath, Storage, Url};
use parquet::{arrow::AsyncArrowWriter, file::properties::WriterProperties};

use crate::file_writer::FileWriter;

/// Whether this session owns the directory or only its newly created parts.
pub(crate) enum Destination {
    Owned(Directory),
    Shared(Directory),
}

pub(crate) struct Directory {
    pub(crate) storage: Storage,
    pub(crate) path: ObjectPath,
    prefix: String,
}

impl Destination {
    pub(crate) async fn open(
        storage: Storage,
        directory: Url,
        schema: &SchemaRef,
        mode: WriteMode,
    ) -> Result<Self> {
        let directory = storage.resolve(&directory)?.as_dir();
        if directory.is_root() {
            return Err(Error::InvalidInput(
                "destination must not be the storage root".into(),
            ));
        }
        let prefix = match mode {
            WriteMode::Create => {
                if storage.inspect_directory(&directory).await? != DirectoryState::Missing {
                    return Err(Error::AlreadyExists(
                        storage.url(directory.as_str())?.to_string(),
                    ));
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
        let directory = Directory {
            storage,
            path: directory,
            prefix,
        };
        Ok(match mode {
            WriteMode::Create => Self::Owned(directory),
            WriteMode::Append => Self::Shared(directory),
            WriteMode::Overwrite => unreachable!(),
        })
    }

    pub(crate) fn directory(&self) -> &Directory {
        match self {
            Self::Owned(directory) | Self::Shared(directory) => directory,
        }
    }

    pub(crate) fn needs_empty_file(&self) -> bool {
        matches!(self, Self::Owned(_))
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
        let directory = self.directory();
        let path = directory
            .path
            .join(&format!(
                "{}{writer}-{sequence:05}.parquet",
                directory.prefix
            ))
            .unwrap();
        directory.storage.create(&path, Bytes::new()).await?;
        let file = FileWriter(directory.storage.writer(&path).await?);
        AsyncArrowWriter::try_new(file, schema, Some(properties)).map_err(Error::backend)
    }
}
