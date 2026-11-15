use std::{io, path::PathBuf, sync::Arc};

use bytes::{Bytes, BytesMut};
use futures::{TryStreamExt, future::BoxFuture};
use object_store::{MultipartUpload, ObjectStore, ObjectStoreExt, PutMode, path::Path};
use open_arrow_sink_core::Result;
use parquet::{arrow::async_writer::AsyncFileWriter, errors::ParquetError};
use tokio::fs::{self, File, OpenOptions};
use url::Url;

use crate::error::backend;

const RESERVATION: &str = ".open-arrow-sink";
const PART_SIZE: usize = 5 * 1024 * 1024;

#[derive(Clone)]
pub(crate) enum Destination {
    Local(PathBuf),
    Remote {
        store: Arc<dyn ObjectStore>,
        prefix: Path,
    },
}

impl Destination {
    pub(crate) fn from_url(url: Url) -> Result<Self> {
        if url.query().is_some() || url.fragment().is_some() {
            return Err(backend(io::Error::other(
                "destination URL cannot contain a query or fragment",
            )));
        }
        if url.scheme() == "file" {
            return url
                .to_file_path()
                .map(Self::Local)
                .map_err(|_| backend(io::Error::other("invalid local file URL")));
        }
        let (store, prefix) =
            object_store::parse_url_opts(&url, std::env::vars()).map_err(backend)?;
        if prefix.as_ref().is_empty() {
            return Err(backend(io::Error::other(
                "destination must have a nonempty object prefix",
            )));
        }
        Ok(Self::Remote {
            store: store.into(),
            prefix,
        })
    }

    pub(crate) async fn reserve(&self) -> Result<()> {
        match self {
            Self::Local(directory) => fs::create_dir(directory).await.map_err(backend),
            Self::Remote { store, prefix } => {
                // Check both an object at the exact path and objects beneath it.
                match store.head(prefix).await {
                    Ok(_) => {
                        return Err(backend(io::Error::new(
                            io::ErrorKind::AlreadyExists,
                            "destination exists",
                        )));
                    }
                    Err(object_store::Error::NotFound { .. }) => {}
                    Err(error) => return Err(backend(error)),
                }
                if store
                    .list(Some(prefix))
                    .try_next()
                    .await
                    .map_err(backend)?
                    .is_some()
                {
                    return Err(backend(io::Error::new(
                        io::ErrorKind::AlreadyExists,
                        "destination prefix exists",
                    )));
                }
                // Conditional creation arbitrates concurrent sessions. Retain the marker
                // on commit, just as a local directory remains after commit.
                store
                    .put_opts(
                        &prefix.clone().join(RESERVATION),
                        Bytes::new().into(),
                        PutMode::Create.into(),
                    )
                    .await
                    .map_err(backend)?;
                Ok(())
            }
        }
    }

    pub(crate) async fn open_file(
        &self,
        index: usize,
        cleanup_on_error: bool,
    ) -> Result<FileWriter> {
        let name = format!("part-{index:05}.parquet");
        match self {
            Self::Local(directory) => Ok(FileWriter::Local(
                OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(directory.join(name))
                    .await
                    .map_err(backend)?,
            )),
            Self::Remote { store, prefix } => {
                let path = prefix.clone().join(name);
                // Claim the name before starting the upload, so a failed claim never
                // gives this session ownership of somebody else's file.
                store
                    .put_opts(&path, Bytes::new().into(), PutMode::Create.into())
                    .await
                    .map_err(backend)?;
                match store.put_multipart(&path).await {
                    Ok(upload) => Ok(FileWriter::Remote {
                        upload,
                        buffer: BytesMut::new(),
                    }),
                    Err(error) => {
                        if cleanup_on_error {
                            let _ = store.delete(&path).await;
                        }
                        Err(backend(error))
                    }
                }
            }
        }
    }

    pub(crate) async fn remove_file(&self, index: usize) -> Result<()> {
        let name = format!("part-{index:05}.parquet");
        match self {
            Self::Local(directory) => match fs::remove_file(directory.join(name)).await {
                Ok(()) => Ok(()),
                Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
                Err(error) => Err(backend(error)),
            },
            Self::Remote { store, prefix } => {
                match store.delete(&prefix.clone().join(name)).await {
                    Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
                    Err(error) => Err(backend(error)),
                }
            }
        }
    }

    pub(crate) async fn remove_reservation(&self, parallel: bool) -> Result<()> {
        match self {
            Self::Local(directory) => match fs::remove_dir(directory).await {
                Err(error)
                    if parallel
                        && matches!(
                            error.kind(),
                            io::ErrorKind::DirectoryNotEmpty | io::ErrorKind::NotFound
                        ) =>
                {
                    Ok(())
                }
                result => result.map_err(backend),
            },
            Self::Remote { store, prefix } => {
                // Only release after all shards have removed their objects. Never
                // recursively delete a prefix; unrelated objects may have appeared.
                let marker = prefix.clone().join(RESERVATION);
                let mut objects = store.list(Some(prefix));
                while let Some(object) = objects.try_next().await.map_err(backend)? {
                    if object.location != marker {
                        return Ok(());
                    }
                }
                match store.delete(&marker).await {
                    Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
                    Err(error) => Err(backend(error)),
                }
            }
        }
    }
}

pub(crate) enum FileWriter {
    Local(File),
    Remote {
        upload: Box<dyn MultipartUpload>,
        buffer: BytesMut,
    },
}

impl FileWriter {
    pub(crate) async fn abort(&mut self) -> Result<()> {
        if let Self::Remote { upload, .. } = self {
            upload.abort().await.map_err(backend)?;
        }
        Ok(())
    }
}

impl AsyncFileWriter for FileWriter {
    fn write(&mut self, mut bytes: Bytes) -> BoxFuture<'_, parquet::errors::Result<()>> {
        Box::pin(async move {
            match self {
                Self::Local(file) => AsyncFileWriter::write(file, bytes).await,
                Self::Remote { upload, buffer } => {
                    // Bound the upload buffer and upload one part at a time. Even a
                    // large encoded row group cannot queue unbounded upload tasks.
                    while !bytes.is_empty() {
                        let size = bytes.len().min(PART_SIZE - buffer.len());
                        buffer.extend_from_slice(&bytes.split_to(size));
                        if buffer.len() == PART_SIZE {
                            upload
                                .put_part(buffer.split().freeze().into())
                                .await
                                .map_err(|e| ParquetError::External(Box::new(e)))?;
                        }
                    }
                    Ok(())
                }
            }
        })
    }

    fn complete(&mut self) -> BoxFuture<'_, parquet::errors::Result<()>> {
        Box::pin(async move {
            match self {
                Self::Local(file) => AsyncFileWriter::complete(file).await,
                Self::Remote { upload, buffer } => {
                    if !buffer.is_empty() {
                        upload
                            .put_part(buffer.split().freeze().into())
                            .await
                            .map_err(|e| ParquetError::External(Box::new(e)))?;
                    }
                    upload
                        .complete()
                        .await
                        .map_err(|e| ParquetError::External(Box::new(e)))?;
                    Ok(())
                }
            }
        })
    }
}
