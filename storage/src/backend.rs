use open_arrow_sink_core::Poison;
use std::{num::NonZeroUsize, ops::Range, sync::Arc};

use bytes::Bytes;
use futures::TryStreamExt;
use open_arrow_sink_core::{Error, Result};
use opendal::{HttpTransporter, Operator};
use url::Url;

use crate::{
    HttpConcurrencyLimitLayer, ObjectPath, StorageConfig,
    error::{self, map_error},
    validation,
};

const DEFAULT_UPLOAD_PART_SIZE: NonZeroUsize = NonZeroUsize::new(4 * 1024 * 1024).unwrap();
const DEFAULT_UPLOAD_CONCURRENCY: NonZeroUsize = NonZeroUsize::new(4).unwrap();

/// The contents of a destination, including virtual object-store directories.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DirectoryState {
    Missing,
    /// An exact non-directory object takes precedence over any children.
    File,
    Empty,
    /// Contains at least one child file or directory.
    NonEmpty,
}

/// An OpenDAL operator bound to a URL namespace. Clones share the configured backend.
/// The operator's root must correspond exactly to `base_url`.
#[derive(Clone, Debug)]
pub struct Storage {
    operator: Operator,
    exported: Operator,
    io_metrics: Vec<open_arrow_sink_core::IoMetrics>,
    http_limit: HttpConcurrencyLimitLayer,
    base_url: Url,
    // Validated once at construction; shared cheaply by storage clones.
    base_path: Arc<str>,
    upload_part_size: NonZeroUsize,
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
        let mut storage = Self {
            exported: operator.clone(),
            operator,
            io_metrics: Vec::new(),
            http_limit: HttpConcurrencyLimitLayer::default(),
            base_url,
            base_path,
            upload_part_size: DEFAULT_UPLOAD_PART_SIZE,
            upload_concurrency: DEFAULT_UPLOAD_CONCURRENCY,
        };
        storage.rebuild_operator();
        Ok(storage)
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

    /// Inspect a non-root destination with or without a trailing slash.
    /// Stops listing at the first child and does not collect the directory.
    /// This is a preflight observation; conditional creation must still claim it.
    pub async fn inspect_directory(&self, path: &ObjectPath) -> Result<DirectoryState> {
        path.require_nonroot()?;
        let directory = path.as_dir();
        let exact = path.as_object();
        let metadata = self.stat(&exact).await?;
        if metadata.as_ref().is_some_and(|metadata| !metadata.is_dir()) {
            return Ok(DirectoryState::File);
        }
        let path = directory.as_str();
        let mut present = metadata.is_some();
        // A shallow listing also sees empty subdirectories on filesystems.
        let children = async {
            let mut entries = self.operator().lister_with(path).limit(1).await?;
            while let Some(entry) = entries.try_next().await? {
                if entry.path() != path {
                    return Ok(true);
                }
                // Some backends include the directory marker itself.
                present = true;
            }
            Ok::<bool, opendal::Error>(false)
        }
        .await;
        match children {
            Ok(true) => return Ok(DirectoryState::NonEmpty),
            Ok(false) => {}
            Err(error) if error.kind() == opendal::ErrorKind::NotFound => {}
            Err(error) => return Err(map_error("list", path, error)),
        }
        // Object stores may only expose an empty marker at the slash URL.
        if present || self.stat(&directory).await?.is_some() {
            Ok(DirectoryState::Empty)
        } else {
            Ok(DirectoryState::Missing)
        }
    }

    /// Validate and add retry/timeout policies after either constructor.
    /// Timeout runs inside retry, giving each attempt its own deadline.
    /// Existing caller-supplied layers, transport, and HTTP budget are preserved.
    /// Repeated calls add layers; configure once before cloning into adapters.
    pub fn with_config(mut self, config: StorageConfig) -> Result<Self> {
        self.operator = config.apply(self.operator)?;
        self.rebuild_operator();
        Ok(self)
    }

    /// Configure the request budget shared by subsequent clones and operators.
    /// Existing clones and operators retain their previous budget.
    /// Upload parts, reads, and metadata requests share this aggregate cap;
    /// size it together with writer and multipart concurrency. The default is 64.
    pub fn with_http_concurrency_limit(mut self, limit: HttpConcurrencyLimitLayer) -> Self {
        self.http_limit = limit;
        self.rebuild_operator();
        self
    }

    /// Replace the provider transport for authentication, instrumentation, or
    /// deterministic tests. The current request budget and operator layers apply.
    pub fn with_http_transport(mut self, transport: HttpTransporter) -> Self {
        let context = self.operator.base_context().with_http_transport(transport);
        self.operator = self.operator.with_context(context);
        self.rebuild_operator();
        self
    }

    /// Configure buffering for streaming uploads. OpenDAL applies the backend's
    /// part-size limits; clones and sink adapters retain this setting.
    pub fn with_upload_part_size(mut self, size: NonZeroUsize) -> Self {
        self.upload_part_size = size;
        self
    }

    /// Apply session upload overrides and byte observers, preserving unset defaults.
    /// Configure a clone to keep the original storage and other sessions unchanged.
    pub fn with_write_options(mut self, options: &open_arrow_sink_core::WriteOptions) -> Self {
        if let Some(size) = options.upload.part_size {
            self.upload_part_size = size;
        }
        if let Some(concurrency) = options.upload.concurrency {
            self.upload_concurrency = concurrency;
        }
        if let Some(metrics) = &options.io_metrics {
            self = self.with_io_metrics(metrics.clone());
        }
        self
    }

    pub(crate) fn without_io_metrics(
        mut self,
    ) -> (Self, Option<open_arrow_sink_core::IoObservers>) {
        let metrics = std::mem::take(&mut self.io_metrics);
        let observers = metrics.into_iter().fold(
            None,
            |all: Option<open_arrow_sink_core::IoObservers>, metrics| {
                Some(match all {
                    Some(all) => all.combine(metrics),
                    None => metrics.into(),
                })
            },
        );
        self.rebuild_operator();
        (self, observers)
    }

    /// Observe this namespace, including native adapters. Repeated handles are ignored.
    /// Configuring another clone leaves existing sessions unchanged.
    pub fn with_io_metrics(
        mut self,
        metrics: impl Into<open_arrow_sink_core::IoObservers>,
    ) -> Self {
        let mut changed = false;
        for metrics in metrics.into().iter() {
            if !self.io_metrics.contains(metrics) {
                self.io_metrics.push(metrics.clone());
                changed = true;
            }
        }
        if changed {
            self.rebuild_operator();
        }
        self
    }

    /// Bound concurrent multipart requests per writer, including native adapters.
    /// Defaults to four; increase this together with the upload-buffer budget.
    pub fn with_upload_concurrency(mut self, concurrency: NonZeroUsize) -> Self {
        self.upload_concurrency = concurrency;
        self
    }

    pub(crate) fn upload_options(
        &self,
        mut options: opendal::options::WriteOptions,
    ) -> opendal::options::WriteOptions {
        options.chunk = Some(self.upload_part_size.get());
        options.concurrent = self.upload_concurrency.get();
        options
    }

    pub fn base_url(&self) -> &Url {
        &self.base_url
    }

    /// Return an operator sharing this storage's transport-level HTTP budget.
    pub fn http_concurrency_limit(&self) -> HttpConcurrencyLimitLayer {
        self.http_limit.clone()
    }

    pub fn operator(&self) -> Operator {
        self.exported.clone()
    }

    fn rebuild_operator(&mut self) {
        // Rebuild from the raw operator so policies stay inside metrics and the
        // provider transport receives exactly one storage-owned limiter.
        let context = self.operator.base_context();
        let transport = self.http_limit.wrap(context.http_transport().clone());
        let mut operator = self
            .operator
            .clone()
            .with_context(context.with_http_transport(transport));
        for metrics in &self.io_metrics {
            operator = operator.layer(crate::IoMetricsLayer(metrics.clone()));
        }
        self.exported = operator;
    }

    /// Decode a URL to an operator-relative path, rejecting paths outside its root.
    pub fn path(&self, url: &Url) -> Result<ObjectPath> {
        validation::relative_path(&self.base_url, &self.base_path, url).map(ObjectPath::new)
    }

    /// Encode a validated relative path as a URL for an external API.
    pub fn url(&self, path: &ObjectPath) -> Result<Url> {
        let path = path.as_str();
        let mut url = self.base_url.clone();
        {
            let mut segments = url
                .path_segments_mut()
                .map_err(|_| Error::InvalidInput("URL cannot contain paths".into()))?;
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

    /// Read exact-object metadata, returning `None` only for NotFound.
    /// Use `inspect_directory` to discover virtual directory prefixes.
    pub async fn stat(&self, path: &ObjectPath) -> Result<Option<opendal::Metadata>> {
        let path = path.as_str();
        match self.operator().stat(path).await {
            Ok(metadata) => Ok(Some(metadata)),
            Err(error) if error.kind() == opendal::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(map_error("stat", path, error)),
        }
    }
    pub async fn read(&self, path: &ObjectPath) -> Result<Bytes> {
        let path = path.as_str();
        self.operator()
            .read(path)
            .await
            .map(|buffer| buffer.to_bytes())
            .map_err(|error| map_error("read", path, error))
    }
    pub async fn read_range(&self, path: &ObjectPath, range: Range<u64>) -> Result<Bytes> {
        let path = path.as_str();
        validation::validate_range(&range)?;
        if range.is_empty() {
            return Ok(Bytes::new());
        }
        self.operator()
            .read_with(path)
            .range(range)
            .await
            .map(|buffer| buffer.to_bytes())
            .map_err(|error| map_error("read", path, error))
    }
    pub async fn write(&self, path: &ObjectPath, bytes: Bytes) -> Result<()> {
        path.require_nonroot()?;
        let path = path.as_str();
        self.operator()
            .write(path, bytes)
            .await
            .map(|_| ())
            .map_err(|error| map_error("write", path, error))
    }
    /// Atomic conditional creation. Unsupported backends fail instead of overwriting.
    pub async fn create(&self, path: &ObjectPath, bytes: Bytes) -> Result<()> {
        path.require_nonroot()?;
        let path = path.as_str();
        self.operator()
            .write_with(path, bytes)
            .if_not_exists(true)
            .await
            .map(|_| ())
            .map_err(|error| map_error("create", path, error))
    }
    pub async fn writer(&self, path: &ObjectPath) -> Result<StorageWriter> {
        path.require_nonroot()?;
        let path = path.as_str();
        let operator = self.operator();
        let writer = operator
            .writer_options(path, self.upload_options(Default::default()))
            .await
            .map_err(|error| map_error("open", path, error))?;
        Ok(StorageWriter {
            inner: Some(writer),
            bytes_written: 0,
            state: Poison::default(),
            path: path.to_owned(),
        })
    }
    pub async fn list(&self, path: &ObjectPath) -> Result<Vec<ObjectPath>> {
        let directory = path.as_dir();
        let path = directory.as_str();
        let mut entries = self.lister(&directory).await?;
        let mut paths = Vec::new();
        while let Some(entry) = entries
            .try_next()
            .await
            .map_err(|error| map_error("list", path, error))?
        {
            if entry.metadata().is_file() {
                validation::validate_path(entry.path())?;
                paths.push(ObjectPath::new(entry.path().to_owned()));
            }
        }
        Ok(paths)
    }
    pub async fn delete(&self, path: &ObjectPath) -> Result<()> {
        path.require_nonroot()?;
        let path = path.as_str();
        error::delete_result(path, self.operator().delete(path).await)
    }
    /// Remove a session-owned namespace. The caller must own every object beneath it.
    /// Refuse to remove an entire operator root.
    pub async fn remove_all(&self, path: &ObjectPath) -> Result<()> {
        path.require_nonroot()?;
        let directory = path.as_dir();
        let path = directory.as_str();
        self.operator()
            .delete_with(path)
            .recursive(true)
            .await
            .map_err(|error| map_error("delete_prefix", path, error))
    }
    async fn lister(&self, path: &ObjectPath) -> Result<opendal::Lister> {
        let path = path.as_str();
        self.operator()
            .lister_with(path)
            .recursive(true)
            .await
            .map_err(|error| map_error("list", path, error))
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
            .ok_or(Error::Poisoned)?
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
            .ok_or(Error::Poisoned)?
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
