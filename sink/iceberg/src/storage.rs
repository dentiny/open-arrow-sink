//! Bridge Iceberg's FileIO contracts to the shared OpenDAL operator.
use std::{
    collections::HashMap,
    ops::Range,
    sync::{Arc, Mutex},
};

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
use uuid::Uuid;

type SharedWriter = Arc<futures::lock::Mutex<StorageWriter>>;

struct Upload {
    path: String,
    writer: SharedWriter,
}

#[derive(Default)]
struct Uploads(Mutex<HashMap<Uuid, Upload>>);
impl std::fmt::Debug for Uploads {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Uploads")
            .field("unfinished", &self.lock().len())
            .finish()
    }
}
impl Uploads {
    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<Uuid, Upload>> {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

/// Inject into `CatalogBuilder::with_storage_factory` so data, metadata, scans,
/// and cleanup all share the same operator. This factory holds a live operator;
/// FileIO serialization is deliberately unsupported instead of losing its state.
#[derive(Clone, Debug)]
pub struct OpenDalStorageFactory {
    storage: Storage,
    // Iceberg's FileWrite has no abort method. Retain unfinished native handles
    // across FileWrite drop so explicit prefix cleanup can still abort them.
    uploads: Arc<Uploads>,
}
impl OpenDalStorageFactory {
    pub fn new(storage: Storage) -> Self {
        Self {
            storage,
            uploads: Arc::default(),
        }
    }
    pub fn file_io(&self) -> iceberg::io::FileIO {
        iceberg::io::FileIOBuilder::new(Arc::new(self.clone())).build()
    }
}

// Operators can contain live credentials, custom layers, or shared memory state.
// Reconstructing a fresh operator on deserialize would silently change storage.
impl serde::Serialize for OpenDalStorageFactory {
    fn serialize<S: serde::Serializer>(&self, _: S) -> std::result::Result<S::Ok, S::Error> {
        Err(crate::error::operator_serialization())
    }
}
impl<'de> serde::Deserialize<'de> for OpenDalStorageFactory {
    fn deserialize<D: serde::Deserializer<'de>>(_: D) -> std::result::Result<Self, D::Error> {
        Err(crate::error::operator_deserialization())
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
            .stat(&location(path)?)
            .await
            .map(|metadata| metadata.is_some())
            .map_err(backend)
    }
    async fn metadata(&self, path: &str) -> Result<FileMetadata> {
        let metadata = self
            .storage
            .stat(&location(path)?)
            .await
            .map_err(backend)?
            .ok_or_else(|| {
                backend(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    format!("Iceberg file does not exist: {path}"),
                ))
            })?;
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
        let url = location(path)?;
        let path = self.storage.path(&url).map_err(backend)?;
        let writer = Arc::new(futures::lock::Mutex::new(
            self.storage.writer(&url).await.map_err(backend)?,
        ));
        let id = Uuid::new_v4();
        self.uploads.lock().insert(
            id,
            Upload {
                path,
                writer: writer.clone(),
            },
        );
        Ok(Box::new(OpenDalWriter {
            writer,
            uploads: self.uploads.clone(),
            id,
        }))
    }
    async fn delete(&self, path: &str) -> Result<()> {
        self.storage.delete(&location(path)?).await.map_err(backend)
    }
    async fn delete_prefix(&self, path: &str) -> Result<()> {
        let url = location(path)?;
        let path = self.storage.path(&url).map_err(backend)?;
        let prefix = if path.is_empty() {
            String::new()
        } else {
            format!("{}/", path.trim_end_matches('/'))
        };
        // Never hold the registry's synchronous mutex across storage I/O.
        let uploads: Vec<_> = self
            .uploads
            .lock()
            .iter()
            .filter(|(_, upload)| upload.path.starts_with(&prefix))
            .map(|(id, upload)| (*id, upload.writer.clone()))
            .collect();
        let mut first_error = None;
        for (id, writer) in uploads {
            if let Err(error) = writer.lock().await.abort().await {
                first_error.get_or_insert_with(|| backend(error));
            }
            self.uploads.lock().remove(&id);
        }
        if let Err(error) = self.storage.remove_all(&url).await {
            first_error.get_or_insert_with(|| backend(error));
        }
        first_error.map_or(Ok(()), Err)
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
struct OpenDalWriter {
    writer: SharedWriter,
    uploads: Arc<Uploads>,
    id: Uuid,
}
#[async_trait]
impl FileWrite for OpenDalWriter {
    async fn write(&mut self, bytes: Bytes) -> Result<()> {
        self.writer.lock().await.write(bytes).await.map_err(backend)
    }
    async fn close(&mut self) -> Result<FileMetadata> {
        let size = self.writer.lock().await.close().await.map_err(backend)?;
        self.uploads.lock().remove(&self.id);
        Ok(FileMetadata { size })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Fixture {
        storage: Storage,
        factory: OpenDalStorageFactory,
    }
    impl Fixture {
        fn new() -> Self {
            let storage = open_arrow_sink_test_support::memory_storage("memory:///warehouse/");
            Self {
                factory: OpenDalStorageFactory::new(storage.clone()),
                storage,
            }
        }
        async fn upload(&self, path: &str, bytes: &'static str) -> Box<dyn FileWrite> {
            let url = self.storage.url(path).unwrap();
            let mut writer = self
                .factory
                .file_io()
                .new_output(url.as_str())
                .unwrap()
                .writer()
                .await
                .unwrap();
            writer.write(bytes.into()).await.unwrap();
            writer
        }
        async fn cleanup(&self) {
            self.factory
                .clone()
                .file_io()
                .delete_prefix(self.storage.url("session-a/").unwrap().as_str())
                .await
                .unwrap();
        }
    }

    #[tokio::test]
    async fn prefix_cleanup_aborts_dropped_native_handles() {
        let fixture = Fixture::new();
        let writer = fixture.upload("session-a/abandoned", "abandoned").await;
        let native = fixture
            .factory
            .uploads
            .lock()
            .values()
            .next()
            .unwrap()
            .writer
            .clone();
        // The native SDK drops FileWrite before requesting prefix cleanup.
        drop(writer);
        fixture.cleanup().await;
        assert!(native.lock().await.write("more".into()).await.is_err());
        assert!(fixture.factory.uploads.lock().is_empty());
        assert!(
            fixture
                .storage
                .stat(&fixture.storage.url("session-a/abandoned").unwrap())
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn prefix_cleanup_aborts_active_uploads_and_preserves_other_sessions() {
        let fixture = Fixture::new();
        let mut active = fixture.upload("session-a/active", "active").await;
        let mut keep = fixture.upload("session-ab/keep", "keep").await;
        fixture.cleanup().await;
        assert_eq!(fixture.factory.uploads.lock().len(), 1);
        assert!(active.write("more".into()).await.is_err());
        assert!(active.close().await.is_err());
        assert!(
            fixture
                .storage
                .stat(&fixture.storage.url("session-a/active").unwrap())
                .await
                .unwrap()
                .is_none()
        );
        keep.write(" more".into()).await.unwrap();
        keep.close().await.unwrap();
        assert!(fixture.factory.uploads.lock().is_empty());
        assert_eq!(
            fixture
                .storage
                .read(&fixture.storage.url("session-ab/keep").unwrap())
                .await
                .unwrap(),
            "keep more"
        );
    }

    #[tokio::test]
    async fn closed_uploads_leave_the_registry_and_prefix_cleanup_is_idempotent() {
        let fixture = Fixture::new();
        let mut writer = fixture.upload("session-a/completed", "complete").await;
        writer.close().await.unwrap();
        assert!(fixture.factory.uploads.lock().is_empty());
        fixture.cleanup().await;
        assert!(
            fixture
                .storage
                .stat(&fixture.storage.url("session-a/completed").unwrap())
                .await
                .unwrap()
                .is_none()
        );
        fixture.cleanup().await;
    }
}
