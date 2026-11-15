use bytes::Bytes;
use futures::future::BoxFuture;
use open_arrow_sink_core::Result;
use open_arrow_sink_storage::{Storage, StorageWriter, Url};
use parquet::{arrow::async_writer::AsyncFileWriter, errors::ParquetError};

const RESERVATION: &str = ".open-arrow-sink";

#[derive(Clone)]
pub(crate) struct Destination {
    storage: Storage,
    directory: Url,
}

impl Destination {
    pub(crate) fn new(storage: Storage, mut directory: Url) -> Result<Self> {
        if storage.path(&directory)?.trim_matches('/').is_empty() {
            return Err(crate::error::backend(std::io::Error::other(
                "destination must have a nonempty path",
            )));
        }
        if !directory.path().ends_with('/') {
            directory.path_segments_mut().unwrap().push("");
        }
        Ok(Self { storage, directory })
    }
    fn child(&self, name: &str) -> Url {
        self.directory.join(name).unwrap()
    }
    pub(crate) async fn reserve(&self) -> Result<()> {
        let mut exact = self.directory.clone();
        exact.path_segments_mut().unwrap().pop_if_empty();
        if self.storage.exists(&exact).await?
            || self.storage.exists(&self.directory).await?
            || !self.storage.list(&self.directory).await?.is_empty()
        {
            return Err(crate::error::backend(std::io::Error::new(
                std::io::ErrorKind::AlreadyExists,
                "destination exists",
            )));
        }
        self.storage
            .create(&self.child(RESERVATION), Bytes::new())
            .await
    }
    pub(crate) async fn open_file(
        &self,
        index: usize,
        cleanup_on_error: bool,
    ) -> Result<FileWriter> {
        let path = self.child(&format!("part-{index:05}.parquet"));
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
        self.storage.delete(&self.child(RESERVATION)).await
    }
    pub(crate) async fn remove_file(&self, index: usize) -> Result<()> {
        self.storage
            .delete(&self.child(&format!("part-{index:05}.parquet")))
            .await
    }
    pub(crate) async fn remove_reservation(&self, _parallel: bool) -> Result<()> {
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
                .map_err(|e| ParquetError::External(Box::new(e)))
        })
    }
    fn complete(&mut self) -> BoxFuture<'_, parquet::errors::Result<()>> {
        Box::pin(async move {
            self.0
                .close()
                .await
                .map(|_| ())
                .map_err(|e| ParquetError::External(Box::new(e)))
        })
    }
}
