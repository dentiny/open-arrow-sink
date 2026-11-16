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
use open_arrow_sink_storage::{ObjectPath, Storage, StorageWriter};
use uuid::Uuid;

type SharedWriter = Arc<futures::lock::Mutex<StorageWriter>>;

struct Upload {
    path: ObjectPath,
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
    // FileIO retains a string location; file handles retain its resolved path
    // here so subsequent reads/writes do not revalidate the same URL.
    resolved: Option<(Arc<str>, ObjectPath)>,
}
impl OpenDalStorageFactory {
    pub fn new(storage: Storage) -> Self {
        Self {
            storage,
            uploads: Arc::default(),
            resolved: None,
        }
    }
    pub fn file_io(&self) -> iceberg::io::FileIO {
        iceberg::io::FileIOBuilder::new(Arc::new(self.clone())).build()
    }

    fn resolve(&self, path: &str) -> Result<ObjectPath> {
        if let Some((location, resolved)) = &self.resolved
            && location.as_ref() == path
        {
            return Ok(resolved.clone());
        }
        self.storage.resolve(&location(path)?).map_err(backend)
    }

    fn for_location(&self, path: &str) -> Result<Self> {
        let resolved = self.resolve(path)?;
        let mut storage = self.clone();
        storage.resolved = Some((Arc::from(path), resolved));
        Ok(storage)
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
            .stat(&self.resolve(path)?)
            .await
            .map(|metadata| metadata.is_some())
            .map_err(backend)
    }
    async fn metadata(&self, path: &str) -> Result<FileMetadata> {
        let metadata = self
            .storage
            .stat(&self.resolve(path)?)
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
        self.storage
            .read(&self.resolve(path)?)
            .await
            .map_err(backend)
    }
    async fn reader(&self, path: &str) -> Result<Box<dyn FileRead>> {
        let path = self.resolve(path)?;
        Ok(Box::new(OpenDalReader {
            storage: self.storage.clone(),
            path,
        }))
    }
    async fn write(&self, path: &str, bytes: Bytes) -> Result<()> {
        self.storage
            .write(&self.resolve(path)?, bytes)
            .await
            .map_err(backend)
    }
    async fn writer(&self, path: &str) -> Result<Box<dyn FileWrite>> {
        let path = self.resolve(path)?;
        let writer = Arc::new(futures::lock::Mutex::new(
            self.storage.writer(&path).await.map_err(backend)?,
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
        self.storage
            .delete(&self.resolve(path)?)
            .await
            .map_err(backend)
    }
    async fn delete_prefix(&self, path: &str) -> Result<()> {
        let path = self.resolve(path)?;
        // Reject the operator root before aborting any active upload.
        if path.is_root() {
            return self.storage.remove_all(&path).await.map_err(backend);
        }
        let prefix = path.as_dir();
        // Never hold the registry's synchronous mutex across storage I/O.
        let uploads: Vec<_> = self
            .uploads
            .lock()
            .iter()
            .filter(|(_, upload)| upload.path.as_str().starts_with(prefix.as_str()))
            .map(|(id, upload)| (*id, upload.writer.clone()))
            .collect();
        let mut first_error = None;
        for (id, writer) in uploads {
            if let Err(error) = writer.lock().await.abort().await {
                first_error.get_or_insert_with(|| backend(error));
            }
            self.uploads.lock().remove(&id);
        }
        if let Err(error) = self.storage.remove_all(&path).await {
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
        Ok(InputFile::new(
            Arc::new(self.for_location(path)?),
            path.to_owned(),
        ))
    }
    fn new_output(&self, path: &str) -> Result<OutputFile> {
        Ok(OutputFile::new(
            Arc::new(self.for_location(path)?),
            path.to_owned(),
        ))
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
    use open_arrow_sink_storage::opendal;

    #[tokio::test]
    async fn prefix_cleanup_aborts_unfinished_writers_without_touching_other_sessions() {
        let storage = Storage::new(
            opendal::Operator::new(opendal::services::Memory::default()).unwrap(),
            "memory:///warehouse/".parse().unwrap(),
        )
        .unwrap();
        let factory = OpenDalStorageFactory::new(storage.clone());
        let file_io = factory.file_io();
        let abandoned = storage.url("session-a/abandoned").unwrap();
        let active = storage.url("session-a/active").unwrap();
        let keep = storage.url("session-ab/keep").unwrap();
        let mut abandoned_writer = file_io
            .new_output(abandoned.as_str())
            .unwrap()
            .writer()
            .await
            .unwrap();
        abandoned_writer.write("abandoned".into()).await.unwrap();
        let native_writer = factory
            .uploads
            .lock()
            .values()
            .next()
            .unwrap()
            .writer
            .clone();
        // The native SDK drops FileWrite before calling delete_prefix on abort.
        drop(abandoned_writer);
        let mut active_writer = file_io
            .new_output(active.as_str())
            .unwrap()
            .writer()
            .await
            .unwrap();
        active_writer.write("active".into()).await.unwrap();
        let mut keep_writer = file_io
            .new_output(keep.as_str())
            .unwrap()
            .writer()
            .await
            .unwrap();
        keep_writer.write("keep".into()).await.unwrap();
        // A factory clone and a new FileIO must share the unfinished-upload registry.
        factory
            .clone()
            .file_io()
            .delete_prefix(storage.url("session-a/").unwrap().as_str())
            .await
            .unwrap();
        // Observe the dropped writer's native handle to prove explicit abort.
        assert!(
            native_writer
                .lock()
                .await
                .write("more".into())
                .await
                .is_err()
        );
        assert_eq!(factory.uploads.lock().len(), 1);
        assert!(active_writer.write("more".into()).await.is_err());
        assert!(active_writer.close().await.is_err());
        assert!(
            storage
                .stat(&storage.resolve(&abandoned).unwrap())
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            storage
                .stat(&storage.resolve(&active).unwrap())
                .await
                .unwrap()
                .is_none()
        );
        keep_writer.write(" more".into()).await.unwrap();
        keep_writer.close().await.unwrap();
        assert!(factory.uploads.lock().is_empty());
        assert_eq!(
            storage
                .read(&storage.resolve(&keep).unwrap())
                .await
                .unwrap(),
            "keep more"
        );
        // Successful close also removes a new upload from the registry.
        let mut completed = file_io
            .new_output(abandoned.as_str())
            .unwrap()
            .writer()
            .await
            .unwrap();
        completed.write("complete".into()).await.unwrap();
        completed.close().await.unwrap();
        assert!(factory.uploads.lock().is_empty());
        // Clean up the completed object, then repeat cleanup on the empty prefix.
        file_io
            .delete_prefix(storage.url("session-a/").unwrap().as_str())
            .await
            .unwrap();
        assert!(
            storage
                .stat(&storage.resolve(&abandoned).unwrap())
                .await
                .unwrap()
                .is_none()
        );
        // Repeated abort cleanup is harmless.
        file_io
            .delete_prefix(storage.url("session-a/").unwrap().as_str())
            .await
            .unwrap();
    }
}
