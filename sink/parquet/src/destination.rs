use bytes::Bytes;
use futures::future::BoxFuture;
use open_arrow_sink_core::{Result, SchemaRef, WriteMode};
use open_arrow_sink_storage::{Storage, StorageWriter, Url};
use parquet::arrow::async_writer::AsyncFileWriter;

const RESERVATION: &str = ".open-arrow-sink";

/// Session-owned parts and optional filesystem staging directory.
pub(crate) struct Destination {
    storage: Storage,
    directory: Url,
    prefix: String,
    remove_directory: bool,
    target: Option<Url>,
    cleanup_on_error: bool,
}
impl Destination {
    pub(crate) async fn open(
        storage: Storage,
        mut directory: Url,
        schema: &SchemaRef,
        mode: WriteMode,
        cleanup_on_error: bool,
    ) -> Result<Self> {
        crate::validation::validate_destination_path(&storage, &directory)?;
        if !directory.path().ends_with('/') {
            directory.path_segments_mut().unwrap().push("");
        }
        let (directory, prefix, target, remove_directory) = match mode {
            WriteMode::Create => (directory, "part-".into(), None, true),
            WriteMode::Append => {
                crate::validation::validate_append_directory(&storage, &directory, schema).await?;
                (
                    directory,
                    format!("part-{}-", uuid::Uuid::new_v4()),
                    None,
                    false,
                )
            }
            WriteMode::Overwrite => {
                crate::validation::validate_overwrite_directory(&storage, &directory).await?;
                let stage = directory
                    .join(&format!("../.open-arrow-sink-{}/", uuid::Uuid::new_v4()))
                    .unwrap();
                (stage, "part-".into(), Some(directory), true)
            }
        };
        let destination = Self {
            storage,
            directory,
            prefix,
            target,
            remove_directory,
            cleanup_on_error,
        };
        if destination.remove_directory {
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
        crate::validation::validate_fresh_destination(&self.storage, &self.directory).await?;
        self.storage
            .create(&self.child(RESERVATION), Bytes::new())
            .await
    }
    pub(crate) async fn open_file(
        &self,
        index: usize,
        cleanup_on_error: bool,
    ) -> Result<FileWriter> {
        let path = self.file(index);
        self.storage.create(&path, Bytes::new()).await?;
        match self.storage.writer(&path).await {
            Ok(writer) => Ok(FileWriter(writer)),
            Err(error) => {
                if cleanup_on_error {
                    let _ = self.storage.delete(&path).await;
                }
                Err(error)
            }
        }
    }
    pub(crate) async fn complete_reservation(&self) -> Result<()> {
        if self.remove_directory {
            self.storage.delete(&self.child(RESERVATION)).await?;
        }
        Ok(())
    }
    pub(crate) async fn remove_file(&self, index: usize) -> Result<()> {
        self.storage.delete(&self.file(index)).await
    }
    pub(crate) async fn remove_reservation(&self, _parallel: bool) -> Result<()> {
        if !self.remove_directory {
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
            .map_err(crate::error::backend)
    }
    pub(crate) async fn publish(&self) -> Result<()> {
        let result = self.publish_inner().await;
        if result.is_err() && self.cleanup_on_error && self.target.is_some() {
            let _ = self.storage.remove_all(&self.directory).await;
        }
        result
    }
    async fn publish_inner(&self) -> Result<()> {
        let Some(target) = &self.target else {
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

pub(crate) struct FileWriter(StorageWriter);
impl FileWriter {
    pub(crate) async fn abort(&mut self) -> Result<()> {
        self.0.abort().await
    }
}
impl AsyncFileWriter for FileWriter {
    fn write(&mut self, bytes: Bytes) -> BoxFuture<'_, parquet::errors::Result<()>> {
        Box::pin(async move {
            self.0
                .write(bytes)
                .await
                .map_err(crate::error::parquet_backend)
        })
    }
    fn complete(&mut self) -> BoxFuture<'_, parquet::errors::Result<()>> {
        Box::pin(async move {
            self.0
                .close()
                .await
                .map(|_| ())
                .map_err(crate::error::parquet_backend)
        })
    }
}
