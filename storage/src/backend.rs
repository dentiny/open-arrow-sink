use open_arrow_sink_core::Poison;
use std::{num::NonZeroUsize, ops::Range, sync::Arc};

use bytes::Bytes;
use futures::TryStreamExt;
use open_arrow_sink_core::Result;
use opendal::{HttpTransporter, Operator};
use url::Url;

use crate::{
    HttpConcurrencyLimitLayer, StorageConfig,
    error::{self, map_error},
    validation,
};

const DEFAULT_UPLOAD_PART_SIZE: usize = 5 * 1024 * 1024;

/// An OpenDAL operator bound to a URL namespace. Clones share the configured backend.
/// The operator's root must correspond exactly to `base_url`.
#[derive(Clone, Debug)]
pub struct Storage {
    operator: Operator,
    // Budget changes wrap this original transport, never an existing limiter.
    base_http_transport: HttpTransporter,
    http_limit: HttpConcurrencyLimitLayer,
    base_url: Url,
    // Validated once at construction; shared cheaply by storage clones.
    base_path: Arc<str>,
    upload_part_size: Option<NonZeroUsize>,
    upload_concurrency: NonZeroUsize,
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
        let base_path = Arc::from(validation::decoded_path(&base_url)?.as_ref());
        let base_http_transport = operator.base_context().http_transport().clone();
        let http_limit = HttpConcurrencyLimitLayer::default();
        let context = operator
            .base_context()
            .with_http_transport(http_limit.wrap(base_http_transport.clone()));
        Ok(Self {
            operator: operator.with_context(context),
            base_http_transport,
            http_limit,
            base_url,
            base_path,
            upload_part_size: None,
            upload_concurrency: NonZeroUsize::new(1).unwrap(),
        })
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
            Operator::new(opendal::services::Fs::default().root("/"))
                .map_err(|error| map_error("configure_storage", base.as_str(), error))?
        } else {
            Operator::from_uri(base.as_str())
                .map_err(|error| map_error("configure_storage", base.as_str(), error))?
        };
        Self::new(operator, base)
    }

    /// Validate a non-root destination and give it directory URL semantics.
    pub fn directory_url(&self, url: &Url) -> Result<Url> {
        self.nonroot_path(url)?;
        let mut directory = url.clone();
        if !directory.path().ends_with('/') {
            directory.path_segments_mut().unwrap().push("");
        }
        Ok(directory)
    }

    /// Check for an exact object, an empty directory, or any objects under a prefix.
    /// This is a preflight check; callers must use conditional creation to claim it.
    pub async fn prefix_exists(&self, url: &Url) -> Result<bool> {
        let directory = self.directory_url(url)?;
        let mut exact = directory.clone();
        exact.path_segments_mut().unwrap().pop_if_empty();
        Ok(self.exists(&exact).await?
            || self.exists(&directory).await?
            || self.has_files(&directory).await?)
    }

    /// Apply optional retry/timeout policies to the supplied OpenDAL operator.
    pub fn try_new(operator: Operator, base_url: Url, config: StorageConfig) -> Result<Self> {
        Self::new(config.apply(operator)?, base_url)
    }

    /// Configure the request budget shared by subsequent clones and operators.
    /// Existing clones and operators retain their previous budget.
    /// Upload parts, reads, and metadata requests share this aggregate cap;
    /// size it together with writer and multipart concurrency. The default is 64.
    pub fn with_http_concurrency_limit(mut self, limit: HttpConcurrencyLimitLayer) -> Self {
        let context = self
            .operator
            .base_context()
            .with_http_transport(limit.wrap(self.base_http_transport.clone()));
        self.operator = self.operator.with_context(context);
        self.http_limit = limit;
        self
    }

    /// Replace the provider transport for authentication, instrumentation, or
    /// deterministic tests. The current request budget and operator layers apply.
    pub fn with_http_transport(mut self, transport: HttpTransporter) -> Self {
        self.base_http_transport = transport;
        let limit = self.http_limit.clone();
        self.with_http_concurrency_limit(limit)
    }

    /// Configure buffering for streaming uploads. OpenDAL applies the backend's
    /// part-size limits; clones and sink adapters retain this setting.
    pub fn with_upload_part_size(mut self, size: NonZeroUsize) -> Self {
        self.upload_part_size = Some(size);
        self
    }

    /// Apply session upload overrides, preserving storage defaults for unset fields.
    /// Other sink write options do not affect storage. Configure a clone to keep
    /// the original storage and other sessions unchanged.
    pub fn with_write_options(mut self, options: &open_arrow_sink_core::WriteOptions) -> Self {
        if let Some(size) = options.upload.part_size {
            self.upload_part_size = Some(size);
        }
        if let Some(concurrency) = options.upload.concurrency {
            self.upload_concurrency = concurrency;
        }
        self
    }

    pub fn upload_part_size(&self) -> Option<NonZeroUsize> {
        self.upload_part_size
    }

    /// Bound concurrent multipart requests per writer, including native adapters.
    /// Defaults to one; increase this together with the upload-buffer budget.
    pub fn with_upload_concurrency(mut self, concurrency: NonZeroUsize) -> Self {
        self.upload_concurrency = concurrency;
        self
    }

    pub fn upload_concurrency(&self) -> NonZeroUsize {
        self.upload_concurrency
    }

    pub(crate) fn upload_options(
        &self,
        mut options: opendal::options::WriteOptions,
    ) -> opendal::options::WriteOptions {
        options.chunk = Some(
            self.upload_part_size
                .map_or(DEFAULT_UPLOAD_PART_SIZE, NonZeroUsize::get),
        );
        options.concurrent = self.upload_concurrency.get();
        options
    }

    pub fn base_url(&self) -> &Url {
        &self.base_url
    }

    /// Return an operator sharing this storage's transport-level HTTP budget.
    pub fn operator(&self) -> Operator {
        self.operator.clone()
    }

    /// Decode a URL to an operator-relative path, rejecting paths outside its root.
    pub fn path(&self, url: &Url) -> Result<String> {
        validation::relative_path(&self.base_url, &self.base_path, url)
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
        let path = self.path(url)?;
        self.operator()
            .exists(&path)
            .await
            .map_err(|error| map_error("exists", &path, error))
    }
    /// Read metadata once, returning `None` only when the object is not found.
    pub async fn stat_optional(&self, url: &Url) -> Result<Option<opendal::Metadata>> {
        let path = self.path(url)?;
        match self.operator().stat(&path).await {
            Ok(metadata) => Ok(Some(metadata)),
            Err(error) if error.kind() == opendal::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(map_error("stat", &path, error)),
        }
    }
    pub async fn stat(&self, url: &Url) -> Result<opendal::Metadata> {
        let path = self.path(url)?;
        self.operator()
            .stat(&path)
            .await
            .map_err(|error| map_error("stat", &path, error))
    }
    pub async fn read(&self, url: &Url) -> Result<Bytes> {
        let path = self.path(url)?;
        self.operator()
            .read(&path)
            .await
            .map(|buffer| buffer.to_bytes())
            .map_err(|error| map_error("read", &path, error))
    }
    pub async fn read_range(&self, url: &Url, range: Range<u64>) -> Result<Bytes> {
        let path = self.path(url)?;
        validation::validate_range(&range)?;
        if range.is_empty() {
            return Ok(Bytes::new());
        }
        self.operator()
            .read_with(&path)
            .range(range)
            .await
            .map(|buffer| buffer.to_bytes())
            .map_err(|error| map_error("read", &path, error))
    }
    pub async fn write(&self, url: &Url, bytes: Bytes) -> Result<()> {
        let path = self.nonroot_path(url)?;
        self.operator()
            .write(&path, bytes)
            .await
            .map(|_| ())
            .map_err(|error| map_error("write", &path, error))
    }
    /// Atomic conditional creation. Unsupported backends fail instead of overwriting.
    pub async fn create(&self, url: &Url, bytes: Bytes) -> Result<()> {
        let path = self.nonroot_path(url)?;
        self.operator()
            .write_with(&path, bytes)
            .if_not_exists(true)
            .await
            .map(|_| ())
            .map_err(|error| map_error("create", &path, error))
    }
    pub async fn writer(&self, url: &Url) -> Result<StorageWriter> {
        let path = self.nonroot_path(url)?;
        let operator = self.operator();
        let writer = operator
            .writer_options(&path, self.upload_options(Default::default()))
            .await
            .map_err(|error| map_error("open", &path, error))?;
        Ok(StorageWriter {
            inner: Some(writer),
            bytes_written: 0,
            state: Poison::default(),
            path,
        })
    }
    pub async fn list(&self, url: &Url) -> Result<Vec<Url>> {
        let path = directory_path(&self.path(url)?);
        let mut entries = self.lister(&path).await?;
        let mut urls = Vec::new();
        while let Some(entry) = entries
            .try_next()
            .await
            .map_err(|error| map_error("list", &path, error))?
        {
            if entry.metadata().is_file() {
                urls.push(self.url(entry.path())?);
            }
        }
        Ok(urls)
    }
    pub async fn delete(&self, url: &Url) -> Result<()> {
        let path = self.nonroot_path(url)?;
        error::delete_result(&path, self.operator().delete(&path).await)
    }
    /// Remove a session-owned namespace. The caller must own every object beneath it.
    /// Refuse to remove an entire operator root.
    pub async fn remove_all(&self, url: &Url) -> Result<()> {
        let path = directory_path(&self.nonroot_path(url)?);
        self.operator()
            .delete_with(&path)
            .recursive(true)
            .await
            .map_err(|error| map_error("delete_prefix", &path, error))
    }
    /// Delete a directory only when it is empty. Never recursively remove its contents.
    pub async fn remove_empty_directory(&self, url: &Url) -> Result<()> {
        if self.has_files(url).await? {
            return Ok(());
        }
        let path = directory_path(&self.nonroot_path(url)?);
        self.operator()
            .delete(&path)
            .await
            .map_err(|error| map_error("delete", &path, error))
    }
    async fn lister(&self, path: &str) -> Result<opendal::Lister> {
        self.operator()
            .lister_with(path)
            .recursive(true)
            .await
            .map_err(|error| map_error("list", path, error))
    }
    async fn has_files(&self, url: &Url) -> Result<bool> {
        let path = directory_path(&self.path(url)?);
        self.lister(&path)
            .await?
            .try_any(|entry| futures::future::ready(entry.metadata().is_file()))
            .await
            .map_err(|error| map_error("list", &path, error))
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
    state: Poison,
    path: String,
}
impl StorageWriter {
    pub async fn write(&mut self, bytes: Bytes) -> Result<()> {
        self.state.begin()?;
        self.inner
            .as_mut()
            .ok_or_else(error::writer_closed)?
            .write(bytes.clone())
            .await
            .map_err(|error| map_error("write", &self.path, error))?;
        self.bytes_written += bytes.len() as u64;
        self.state.complete();
        Ok(())
    }
    pub async fn close(&mut self) -> Result<u64> {
        self.state.begin()?;
        self.inner
            .as_mut()
            .ok_or_else(error::writer_closed)?
            .close()
            .await
            .map_err(|error| map_error("close", &self.path, error))?;
        self.inner = None;
        Ok(self.bytes_written)
    }
    pub async fn abort(&mut self) -> Result<()> {
        self.state.poison();
        if let Some(mut writer) = self.inner.take() {
            error::abort_result(&self.path, writer.abort().await)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use open_arrow_sink_core::{UploadConfig, WriteOptions};
    use opendal::options::WriteOptions as NativeWriteOptions;

    #[test]
    fn session_upload_overrides_preserve_defaults_and_other_sessions() {
        let storage = Storage::new(
            Operator::new(opendal::services::Memory::default()).unwrap(),
            "memory:///warehouse/".parse().unwrap(),
        )
        .unwrap();
        let native = storage.upload_options(NativeWriteOptions::default());
        assert_eq!(native.chunk, Some(DEFAULT_UPLOAD_PART_SIZE));
        assert_eq!(native.concurrent, 1);

        let storage = storage
            .with_upload_part_size(NonZeroUsize::new(8 * 1024 * 1024).unwrap())
            .with_upload_concurrency(NonZeroUsize::new(2).unwrap());
        for (size, concurrency) in [
            (Some(16), None),
            (None, Some(4)),
            (Some(16), Some(4)),
            (None, None),
        ] {
            let options = WriteOptions::default().with_upload(
                UploadConfig::default()
                    .with_part_size(size.and_then(NonZeroUsize::new))
                    .with_concurrency(concurrency.and_then(NonZeroUsize::new)),
            );
            let session = storage.clone().with_write_options(&options);
            let native = session.upload_options(NativeWriteOptions::default());
            assert_eq!(native.chunk, Some(size.unwrap_or(8 * 1024 * 1024)));
            assert_eq!(native.concurrent, concurrency.unwrap_or(2));
            assert_eq!(storage.upload_part_size().unwrap().get(), 8 * 1024 * 1024);
            assert_eq!(storage.upload_concurrency().get(), 2);
        }
    }
}
