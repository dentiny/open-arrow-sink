//! Bridge Iceberg's FileIO contracts to the shared OpenDAL operator.
use std::{ops::Range, sync::Arc};

use crate::{error::storage_backend as backend, validation::storage_location as location};
use bytes::Bytes;
use futures::{StreamExt, TryStreamExt, stream::BoxStream};
use iceberg::{
    Result,
    io::{
        FileMetadata, FileRead, FileWrite, InputFile, OutputFile, Storage as IcebergStorage,
        StorageConfig, StorageFactory,
    },
};
use open_arrow_sink_core::async_trait;
use open_arrow_sink_storage::{ObjectPath, Storage, StorageWriter};
// Bound deletion tasks independently of stream length and the HTTP request budget.
const DELETE_CONCURRENCY: usize = 32;

/// Inject into `CatalogBuilder::with_storage_factory` so data, metadata, scans,
/// and cleanup all share the same operator. This factory holds a live operator;
/// FileIO serialization is deliberately unsupported instead of losing its state.
#[derive(Clone, Debug)]
pub struct OpenDalStorageFactory {
    storage: Storage,
}
impl OpenDalStorageFactory {
    pub fn new(storage: Storage) -> Self {
        Self { storage }
    }
    pub fn file_io(&self) -> iceberg::io::FileIO {
        iceberg::io::FileIOBuilder::new(Arc::new(self.clone())).build()
    }

    fn path(&self, path: &str) -> Result<ObjectPath> {
        self.storage.path(&location(path)?).map_err(backend)
    }
}

// Operators can contain live credentials, custom layers, or shared memory state.
// Reconstructing a fresh operator on deserialize would silently change storage.
impl serde::Serialize for OpenDalStorageFactory {
    fn serialize<S: serde::Serializer>(&self, _: S) -> std::result::Result<S::Ok, S::Error> {
        Err(<S::Error as serde::ser::Error>::custom(
            "live OpenDAL operators cannot be serialized",
        ))
    }
}
impl<'de> serde::Deserialize<'de> for OpenDalStorageFactory {
    fn deserialize<D: serde::Deserializer<'de>>(_: D) -> std::result::Result<Self, D::Error> {
        Err(<D::Error as serde::de::Error>::custom(
            "construct OpenDAL storage with a configured operator",
        ))
    }
}

#[typetag::serde(name = "open-arrow-sink-opendal-factory")]
impl StorageFactory for OpenDalStorageFactory {
    fn build(&self, _: &StorageConfig) -> Result<Arc<dyn IcebergStorage>> {
        Ok(Arc::new(self.clone()))
    }
}
#[async_trait]
#[typetag::serde(name = "open-arrow-sink-opendal")]
impl IcebergStorage for OpenDalStorageFactory {
    async fn exists(&self, path: &str) -> Result<bool> {
        self.storage
            .stat(&self.path(path)?)
            .await
            .map(|metadata| metadata.is_some())
            .map_err(backend)
    }
    async fn metadata(&self, path: &str) -> Result<FileMetadata> {
        let metadata = self
            .storage
            .metadata(&self.path(path)?)
            .await
            .map_err(backend)?;
        Ok(FileMetadata {
            size: metadata.content_length(),
        })
    }
    async fn read(&self, path: &str) -> Result<Bytes> {
        self.storage.read(&self.path(path)?).await.map_err(backend)
    }
    async fn reader(&self, path: &str) -> Result<Box<dyn FileRead>> {
        let path = self.path(path)?;
        Ok(Box::new(OpenDalReader {
            storage: self.storage.clone(),
            path,
        }))
    }
    async fn write(&self, path: &str, bytes: Bytes) -> Result<()> {
        self.storage
            .write(&self.path(path)?, bytes)
            .await
            .map_err(backend)
    }
    async fn writer(&self, path: &str) -> Result<Box<dyn FileWrite>> {
        let path = self.path(path)?;
        Ok(Box::new(OpenDalWriter {
            writer: self.storage.writer(&path).await.map_err(backend)?,
        }))
    }
    async fn delete(&self, path: &str) -> Result<()> {
        self.storage
            .delete(&self.path(path)?)
            .await
            .map_err(backend)
    }
    async fn delete_prefix(&self, path: &str) -> Result<()> {
        self.storage
            .remove_all(&self.path(path)?)
            .await
            .map_err(backend)
    }
    async fn delete_stream(&self, paths: BoxStream<'static, String>) -> Result<()> {
        paths
            .map(Ok)
            .try_for_each_concurrent(DELETE_CONCURRENCY, |path| async move {
                self.delete(&path).await
            })
            .await
    }
    fn new_input(&self, path: &str) -> Result<InputFile> {
        self.path(path)?;
        Ok(InputFile::new(Arc::new(self.clone()), path.to_owned()))
    }
    fn new_output(&self, path: &str) -> Result<OutputFile> {
        self.path(path)?;
        Ok(OutputFile::new(Arc::new(self.clone()), path.to_owned()))
    }
}
struct OpenDalReader {
    storage: Storage,
    path: ObjectPath,
}
#[async_trait]
impl FileRead for OpenDalReader {
    async fn read(&self, range: Range<u64>) -> Result<Bytes> {
        self.storage
            .read_range(&self.path, range)
            .await
            .map_err(backend)
    }
}
struct OpenDalWriter {
    writer: StorageWriter,
}
#[async_trait]
impl FileWrite for OpenDalWriter {
    async fn write(&mut self, bytes: Bytes) -> Result<()> {
        self.writer.write(bytes).await.map_err(backend)
    }
    async fn close(&mut self) -> Result<FileMetadata> {
        let size = self.writer.close().await.map_err(backend)?;
        Ok(FileMetadata { size })
    }
}
