use std::ops::Range;

use bytes::Bytes;
use futures::TryStreamExt;
use open_arrow_sink_core::Result;
use opendal::Operator;
use url::Url;

use crate::{
    error::{self, backend},
    validation,
};

/// An OpenDAL operator bound to a URL namespace. Clones share the configured backend.
/// The operator's root must correspond exactly to `base_url`.
#[derive(Clone, Debug)]
pub struct Storage {
    operator: Operator,
    base_url: Url,
}

impl Storage {
    /// Filesystem operators must leave `atomic_write_dir` unset: in OpenDAL
    /// 0.59 staged writes do not atomically enforce `if_not_exists` at publication.
    pub fn new(operator: Operator, mut base_url: Url) -> Result<Self> {
        validation::validate_url(&base_url)?;
        if !base_url.path().ends_with('/') {
            let path = format!("{}/", base_url.path());
            base_url.set_path(&path);
        }
        validation::decoded_path(&base_url)?;
        Ok(Self { operator, base_url })
    }

    /// Resolve a URL using enabled OpenDAL services. For custom credentials,
    /// endpoints, or shared memory stores, pass a configured operator to `new`.
    /// Backend features are additive; no cloud services are enabled by default.
    pub fn from_url(url: &Url) -> Result<Self> {
        validation::validate_url(url)?;
        let mut base = url.clone();
        base.set_path("/");
        let operator = if url.scheme() == "file" {
            validation::validate_file_url(url)?;
            // Keep create_new atomic. OpenDAL Fs atomic_write_dir checks for
            // existence before rename, which is insufficient for competing claims.
            Operator::new(opendal::services::Fs::default().root("/")).map_err(backend)?
        } else {
            Operator::from_uri(base.as_str()).map_err(backend)?
        };
        Self::new(operator, base)
    }

    pub fn base_url(&self) -> &Url {
        &self.base_url
    }
    pub fn operator(&self) -> &Operator {
        &self.operator
    }

    /// Decode a URL to an operator-relative path, rejecting paths outside its root.
    pub fn path(&self, url: &Url) -> Result<String> {
        validation::relative_path(&self.base_url, url)
    }

    pub fn url(&self, path: &str) -> Result<Url> {
        validation::validate_path(path)?;
        let mut url = self.base_url.clone();
        {
            let mut segments = url
                .path_segments_mut()
                .map_err(|_| error::url_without_paths())?;
            segments.pop_if_empty();
            for segment in path
                .trim_end_matches('/')
                .split('/')
                .filter(|s| !s.is_empty())
            {
                segments.push(segment);
            }
            if path.ends_with('/') {
                segments.push("");
            }
        }
        Ok(url)
    }

    pub async fn exists(&self, url: &Url) -> Result<bool> {
        self.operator
            .exists(&self.path(url)?)
            .await
            .map_err(backend)
    }
    pub async fn stat(&self, url: &Url) -> Result<opendal::Metadata> {
        self.operator.stat(&self.path(url)?).await.map_err(backend)
    }
    pub async fn read(&self, url: &Url) -> Result<Bytes> {
        self.operator
            .read(&self.path(url)?)
            .await
            .map(|buffer| buffer.to_bytes())
            .map_err(backend)
    }
    pub async fn read_range(&self, url: &Url, range: Range<u64>) -> Result<Bytes> {
        let path = self.path(url)?;
        validation::validate_range(&range)?;
        if range.is_empty() {
            return Ok(Bytes::new());
        }
        self.operator
            .read_with(&path)
            .range(range)
            .await
            .map(|buffer| buffer.to_bytes())
            .map_err(backend)
    }
    pub async fn write(&self, url: &Url, bytes: Bytes) -> Result<()> {
        self.operator
            .write(&self.nonroot_path(url)?, bytes)
            .await
            .map(|_| ())
            .map_err(backend)
    }
    /// Atomic conditional creation. Unsupported backends fail instead of overwriting.
    pub async fn create(&self, url: &Url, bytes: Bytes) -> Result<()> {
        self.operator
            .write_with(&self.nonroot_path(url)?, bytes)
            .if_not_exists(true)
            .await
            .map(|_| ())
            .map_err(backend)
    }
    pub async fn writer(&self, url: &Url) -> Result<StorageWriter> {
        let writer = self
            .operator
            .writer_with(&self.nonroot_path(url)?)
            .chunk(5 * 1024 * 1024)
            .concurrent(1)
            .await
            .map_err(backend)?;
        Ok(StorageWriter {
            inner: Some(writer),
            bytes_written: 0,
            active: true,
            filesystem: self.operator.info().scheme() == "fs",
        })
    }
    pub async fn list(&self, url: &Url) -> Result<Vec<Url>> {
        let path = directory_path(&self.path(url)?);
        let mut entries = self
            .operator
            .lister_with(&path)
            .recursive(true)
            .await
            .map_err(backend)?;
        let mut urls = Vec::new();
        while let Some(entry) = entries.try_next().await.map_err(backend)? {
            if entry.metadata().is_file() {
                urls.push(self.url(entry.path())?);
            }
        }
        Ok(urls)
    }
    pub async fn delete(&self, url: &Url) -> Result<()> {
        error::delete_result(self.operator.delete(&self.nonroot_path(url)?).await)
    }
    /// Remove a session-owned namespace. The caller must own every object beneath it.
    /// Refuse to remove an entire operator root.
    pub async fn remove_all(&self, url: &Url) -> Result<()> {
        self.operator
            .delete_with(&directory_path(&self.nonroot_path(url)?))
            .recursive(true)
            .await
            .map_err(backend)
    }
    /// Delete a directory only when it is empty. Never recursively remove its contents.
    pub async fn remove_empty_directory(&self, url: &Url) -> Result<()> {
        if !self.list(url).await?.is_empty() {
            return Ok(());
        }
        self.operator
            .delete(&directory_path(&self.nonroot_path(url)?))
            .await
            .map_err(backend)
    }
    fn nonroot_path(&self, url: &Url) -> Result<String> {
        let path = self.path(url)?;
        validation::validate_nonroot_path(&path)?;
        Ok(path)
    }
}

fn directory_path(path: &str) -> String {
    if path.is_empty() {
        String::new()
    } else {
        format!("{}/", path.trim_end_matches('/'))
    }
}
/// A streaming byte writer with explicit close/abort and terminal failure semantics.
/// Drop never commits or performs async cleanup.
pub struct StorageWriter {
    inner: Option<opendal::Writer>,
    bytes_written: u64,
    active: bool,
    filesystem: bool,
}
impl StorageWriter {
    pub async fn write(&mut self, bytes: Bytes) -> Result<()> {
        validation::validate_writer_active(self.active)?;
        self.active = false;
        self.inner
            .as_mut()
            .ok_or_else(error::writer_closed)?
            .write(bytes.clone())
            .await
            .map_err(backend)?;
        self.bytes_written += bytes.len() as u64;
        self.active = true;
        Ok(())
    }
    pub async fn close(&mut self) -> Result<u64> {
        validation::validate_writer_active(self.active)?;
        self.active = false;
        self.inner
            .as_mut()
            .ok_or_else(error::writer_closed)?
            .close()
            .await
            .map_err(backend)?;
        self.inner = None;
        Ok(self.bytes_written)
    }
    pub async fn abort(&mut self) -> Result<()> {
        self.active = false;
        if let Some(mut writer) = self.inner.take() {
            error::abort_result(writer.abort().await, self.filesystem)?;
        }
        Ok(())
    }
}
