//! Bridge Iceberg's FileIO contracts to the shared OpenDAL operator.
use std::{ops::Range, sync::Arc};

use crate::{error::storage_backend as backend, validation::storage_location as location};
use bytes::Bytes;
use futures::{StreamExt, stream::BoxStream};
use iceberg::{
    Result,
    io::{
        FileMetadata, FileRead, FileWrite, InputFile, OutputFile, Storage as IcebergStorage,
        StorageConfig, StorageFactory,
    },
};
use open_arrow_sink_core::async_trait;
use open_arrow_sink_storage::{Storage, StorageWriter, Url};

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
}
#[derive(Clone, Debug)]
struct OpenDalStorage {
    storage: Storage,
}

// Operators can contain live credentials, custom layers, or shared memory state.
// Reconstructing a fresh operator on deserialize would silently change storage.
macro_rules! runtime_only {
    ($type:ty) => {
        impl serde::Serialize for $type {
            fn serialize<S: serde::Serializer>(
                &self,
                _: S,
            ) -> std::result::Result<S::Ok, S::Error> {
                Err(crate::error::operator_serialization())
            }
        }
        impl<'de> serde::Deserialize<'de> for $type {
            fn deserialize<D: serde::Deserializer<'de>>(
                _: D,
            ) -> std::result::Result<Self, D::Error> {
                Err(crate::error::operator_deserialization())
            }
        }
    };
}
runtime_only!(OpenDalStorageFactory);
runtime_only!(OpenDalStorage);

#[typetag::serde(name = "open-arrow-sink-opendal-factory")]
impl StorageFactory for OpenDalStorageFactory {
    fn build(&self, _: &StorageConfig) -> Result<Arc<dyn IcebergStorage>> {
        Ok(Arc::new(OpenDalStorage {
            storage: self.storage.clone(),
        }))
    }
}
#[async_trait]
#[typetag::serde(name = "open-arrow-sink-opendal")]
impl IcebergStorage for OpenDalStorage {
    async fn exists(&self, path: &str) -> Result<bool> {
        self.storage.exists(&location(path)?).await.map_err(backend)
    }
    async fn metadata(&self, path: &str) -> Result<FileMetadata> {
        let metadata = self.storage.stat(&location(path)?).await.map_err(backend)?;
        Ok(FileMetadata {
            size: metadata.content_length(),
        })
    }
    async fn read(&self, path: &str) -> Result<Bytes> {
        self.storage.read(&location(path)?).await.map_err(backend)
    }
    async fn reader(&self, path: &str) -> Result<Box<dyn FileRead>> {
        let url = location(path)?;
        self.storage.path(&url).map_err(backend)?;
        Ok(Box::new(OpenDalReader {
            storage: self.storage.clone(),
            url,
        }))
    }
    async fn write(&self, path: &str, bytes: Bytes) -> Result<()> {
        self.storage
            .write(&location(path)?, bytes)
            .await
            .map_err(backend)
    }
    async fn writer(&self, path: &str) -> Result<Box<dyn FileWrite>> {
        Ok(Box::new(OpenDalWriter(
            self.storage
                .writer(&location(path)?)
                .await
                .map_err(backend)?,
        )))
    }
    async fn delete(&self, path: &str) -> Result<()> {
        self.storage.delete(&location(path)?).await.map_err(backend)
    }
    async fn delete_prefix(&self, path: &str) -> Result<()> {
        self.storage
            .remove_all(&location(path)?)
            .await
            .map_err(backend)
    }
    async fn delete_stream(&self, mut paths: BoxStream<'static, String>) -> Result<()> {
        while let Some(path) = paths.next().await {
            self.delete(&path).await?;
        }
        Ok(())
    }
    fn new_input(&self, path: &str) -> Result<InputFile> {
        self.storage.path(&location(path)?).map_err(backend)?;
        Ok(InputFile::new(Arc::new(self.clone()), path.to_owned()))
    }
    fn new_output(&self, path: &str) -> Result<OutputFile> {
        self.storage.path(&location(path)?).map_err(backend)?;
        Ok(OutputFile::new(Arc::new(self.clone()), path.to_owned()))
    }
}
struct OpenDalReader {
    storage: Storage,
    url: Url,
}
#[async_trait]
impl FileRead for OpenDalReader {
    async fn read(&self, range: Range<u64>) -> Result<Bytes> {
        self.storage
            .read_range(&self.url, range)
            .await
            .map_err(backend)
    }
}
struct OpenDalWriter(StorageWriter);
#[async_trait]
impl FileWrite for OpenDalWriter {
    async fn write(&mut self, bytes: Bytes) -> Result<()> {
        self.0.write(bytes).await.map_err(backend)
    }
    async fn close(&mut self) -> Result<FileMetadata> {
        Ok(FileMetadata {
            size: self.0.close().await.map_err(backend)?,
        })
    }
}
