use std::sync::atomic::{AtomicUsize, Ordering};

use bytes::Bytes;
use open_arrow_sink_core::{Error, Result, SchemaRef, WriteMode};
use open_arrow_sink_storage::{Storage, Url};
use parquet::{arrow::AsyncArrowWriter, file::properties::WriterProperties};

use crate::file_writer::FileWriter;

const RESERVATION: &str = ".open-arrow-sink";

/// Session-owned parts and optional filesystem staging directory.
pub(crate) struct Destination {
    storage: Storage,
    directory: Url,
    mode: Mode,
    next_file: AtomicUsize,
    cleanup_on_error: bool,
}
enum Mode {
    Create,
    Append { prefix: String },
    Overwrite { target: Url },
}

impl Mode {
    fn owns_directory(&self) -> bool {
        !matches!(self, Self::Append { .. })
    }

    fn prefix(&self) -> &str {
        match self {
            Self::Append { prefix } => prefix,
            Self::Create | Self::Overwrite { .. } => "part-",
        }
    }
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
        let (directory, mode) = match mode {
            WriteMode::Create => (directory, Mode::Create),
            WriteMode::Append => {
                crate::validation::validate_append_directory(&storage, &directory, schema).await?;
                (
                    directory,
                    Mode::Append {
                        prefix: format!("part-{}-", uuid::Uuid::new_v4()),
                    },
                )
            }
            WriteMode::Overwrite => {
                crate::validation::validate_overwrite_directory(&storage, &directory).await?;
                let stage = directory
                    .join(&format!("../.open-arrow-sink-{}/", uuid::Uuid::new_v4()))
                    .unwrap();
                (stage, Mode::Overwrite { target: directory })
            }
        };
        let destination = Self {
            storage,
            directory,
            mode,
            next_file: AtomicUsize::new(0),
            cleanup_on_error,
        };
        if destination.mode.owns_directory() {
            destination.reserve().await?;
        }
        Ok(destination)
    }
    fn child(&self, name: &str) -> Url {
        self.directory.join(name).unwrap()
    }
    fn file(&self, index: usize) -> Url {
        self.child(&format!("{}{index:05}.parquet", self.mode.prefix()))
    }
    async fn reserve(&self) -> Result<()> {
        if self.storage.prefix_exists(&self.directory).await? {
            return Err(Error::DestinationExists);
        }
        self.storage
            .create(&self.child(RESERVATION), Bytes::new())
            .await
    }
    pub(crate) async fn open_writer(
        &self,
        schema: SchemaRef,
        properties: WriterProperties,
    ) -> Result<(usize, AsyncArrowWriter<FileWriter>)> {
        let index = self.next_file.fetch_add(1, Ordering::Relaxed);
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
        result.map(|writer| (index, writer))
    }
    async fn complete_reservation(&self) -> Result<()> {
        if self.mode.owns_directory() {
            self.storage.delete(&self.child(RESERVATION)).await?;
        }
        Ok(())
    }
    pub(crate) async fn remove_file(&self, index: usize) -> Result<()> {
        self.storage.delete(&self.file(index)).await
    }
    pub(crate) async fn abort(&self) -> Result<()> {
        if !self.cleanup_on_error || !self.mode.owns_directory() {
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
    async fn rename_directory(&self, from: &Url, to: &Url) -> Result<()> {
        // Fs supports directory rename at the backend; omit trailing slashes
        // because OpenDAL's public rename path check requires file-shaped paths.
        let from = self.storage.path(from)?;
        let to = self.storage.path(to)?;
        self.storage
            .operator()
            .rename(from.trim_end_matches('/'), to.trim_end_matches('/'))
            .await
            .map_err(Error::backend)
    }
    pub(crate) async fn publish(&self) -> Result<()> {
        // Release the session claim once, after all data writers have finished.
        self.complete_reservation().await?;
        let result = self.publish_inner().await;
        if result.is_err() && self.cleanup_on_error && matches!(self.mode, Mode::Overwrite { .. }) {
            let _ = self.storage.remove_all(&self.directory).await;
        }
        result
    }
    async fn publish_inner(&self) -> Result<()> {
        let Mode::Overwrite { target } = &self.mode else {
            return Ok(());
        };
        let backup = target
            .join(&format!(
                "../.open-arrow-sink-old-{}/",
                uuid::Uuid::new_v4()
            ))
            .unwrap();
        let had_target = self.storage.exists(target).await?;
        if had_target {
            self.rename_directory(target, &backup).await?;
        }
        if let Err(error) = self.rename_directory(&self.directory, target).await {
            if had_target {
                let _ = self.rename_directory(&backup, target).await;
            }
            return Err(error);
        }
        if had_target {
            let _ = self.storage.remove_all(&backup).await;
        }
        Ok(())
    }
}
