use bytes::Bytes;
use open_arrow_sink_core::{Result, SchemaRef, WriteMode};
use open_arrow_sink_storage::{Storage, Url};
use parquet::{arrow::AsyncArrowWriter, file::properties::WriterProperties};

use crate::file_writer::FileWriter;

const RESERVATION: &str = ".open-arrow-sink";

/// Session-owned parts in a destination directory.
pub(crate) struct Destination {
    storage: Storage,
    directory: Url,
    prefix: String,
    // Create owns the destination. Append shares an existing directory and must
    // preserve its reservation. Only owners create/remove the reservation and
    // clean up the empty directory.
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
        let directory = storage.directory_url(&directory)?;
        let (prefix, owns_directory) = match mode {
            WriteMode::Create => ("part-".into(), true),
            WriteMode::Append => {
                crate::validation::validate_append_directory(&storage, &directory, schema).await?;
                (format!("part-{}-", uuid::Uuid::new_v4()), false)
            }
            WriteMode::Overwrite => return Err(crate::error::overwrite_unsupported()),
        };
        let destination = Self {
            storage,
            directory,
            prefix,
            owns_directory,
            cleanup_on_error,
        };
        if destination.owns_directory {
            destination.reserve().await?;
        }
        Ok(destination)
    }
    fn child(&self, name: &str) -> Url {
        self.directory.join(name).unwrap()
    }
    fn file(&self, index: usize) -> Url {
        self.child(&format!("{}{index:05}.parquet", self.prefix))
    }
    async fn reserve(&self) -> Result<()> {
        if self.storage.prefix_exists(&self.directory).await? {
            return Err(crate::error::destination_exists());
        }
        self.storage
            .create(&self.child(RESERVATION), Bytes::new())
            .await
    }
    pub(crate) async fn reserve_file(&self, index: usize) -> Result<()> {
        self.storage.create(&self.file(index), Bytes::new()).await
    }
    pub(crate) async fn open_reserved_writer(
        &self,
        index: usize,
        schema: SchemaRef,
        properties: WriterProperties,
    ) -> Result<AsyncArrowWriter<FileWriter>> {
        let path = self.file(index);
        let result = async {
            let file = FileWriter(self.storage.writer(&path).await?);
            AsyncArrowWriter::try_new(file, schema, Some(properties)).map_err(crate::error::backend)
        }
        .await;
        if result.is_err() && self.cleanup_on_error {
            let _ = self.storage.delete(&path).await;
        }
        result
    }
    pub(crate) fn needs_empty_file(&self) -> bool {
        self.owns_directory
    }
    pub(crate) async fn write_empty_file(
        &self,
        schema: SchemaRef,
        properties: WriterProperties,
    ) -> Result<()> {
        self.reserve_file(0).await?;
        let mut writer = self.open_reserved_writer(0, schema, properties).await?;
        let result = writer.finish().await.map_err(crate::error::backend);
        if result.is_err() && self.cleanup_on_error {
            let _ = writer.into_inner().abort().await;
            let _ = self.remove_file(0).await;
        }
        result.map(|_| ())
    }
    pub(crate) async fn cleanup_reservation(&self) -> Result<()> {
        if self.cleanup_on_error {
            self.remove_reservation().await?;
        }
        Ok(())
    }
    pub(crate) async fn complete_reservation(&self) -> Result<()> {
        if self.owns_directory {
            self.storage.delete(&self.child(RESERVATION)).await?;
        }
        Ok(())
    }
    pub(crate) async fn remove_file(&self, index: usize) -> Result<()> {
        self.storage.delete(&self.file(index)).await
    }
    pub(crate) async fn remove_reservation(&self) -> Result<()> {
        if !self.owns_directory {
            return Ok(());
        }
        let marker = self.child(RESERVATION);
        if self
            .storage
            .list(&self.directory)
            .await?
            .iter()
            .any(|url| url != &marker)
        {
            return Ok(());
        }
        self.storage.delete(&marker).await?;
        self.storage.remove_empty_directory(&self.directory).await
    }
}
