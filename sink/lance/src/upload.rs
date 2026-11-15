//! Preserve OpenDAL object-store operations while configuring upload part sizes.
use std::{fmt, ops::Range, sync::Arc};

use bytes::Bytes;
use futures::{FutureExt, stream::BoxStream};
use object_store::{
    Attribute, CopyOptions, Extensions, GetOptions, GetResult, ListResult, MultipartUpload,
    ObjectMeta, ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult, RenameOptions,
    Result, UploadPart, path::Path,
};
use object_store_opendal::{IntoSendFuture, OpendalStore};
use open_arrow_sink_core::async_trait;
use open_arrow_sink_storage::opendal;
use opendal::raw::percent_decode_path;
use opendal::{Buffer, Operator, Writer, options::WriteOptions};
use tokio::sync::{Mutex, oneshot};

// Preserve the adapter's existing per-writer upload concurrency.
const UPLOAD_CONCURRENCY: usize = 8;

#[derive(Debug)]
pub(super) struct UploadStore {
    inner: OpendalStore,
    operator: Operator,
    part_size: usize,
}

impl UploadStore {
    pub(super) fn new(operator: Operator, part_size: usize) -> Self {
        Self {
            inner: OpendalStore::new(operator.clone()),
            operator,
            part_size,
        }
    }
}

impl fmt::Display for UploadStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.inner.fmt(f)
    }
}

#[async_trait]
#[deny(clippy::missing_trait_methods)]
impl ObjectStore for UploadStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        let mut options = WriteOptions {
            chunk: Some(self.part_size),
            concurrent: UPLOAD_CONCURRENCY,
            ..Default::default()
        };
        // Match the upstream adapter's metadata handling; conditional single
        // puts still go through the unchanged adapter for Lance commits.
        for (key, value) in &opts.attributes {
            match key {
                Attribute::CacheControl => options.cache_control = Some(value.to_string()),
                Attribute::ContentDisposition => {
                    options.content_disposition = Some(value.to_string());
                }
                Attribute::ContentEncoding => options.content_encoding = Some(value.to_string()),
                Attribute::ContentType => options.content_type = Some(value.to_string()),
                Attribute::Metadata(key) => {
                    options
                        .user_metadata
                        .get_or_insert_default()
                        .insert(key.to_string(), value.to_string());
                }
                _ => {}
            }
        }
        let decoded = percent_decode_path(location.as_ref());
        let writer = self
            .operator
            .writer_options(&decoded, options)
            .into_send()
            .await
            .map_err(|error| upload_error(error, location))?;
        Ok(Box::new(Upload {
            writer: Arc::new(Mutex::new(writer)),
            location: location.clone(),
            next: None,
        }))
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        self.inner.get_opts(location, options).await
    }

    async fn get_ranges(&self, location: &Path, ranges: &[Range<u64>]) -> Result<Vec<Bytes>> {
        self.inner.get_ranges(location, ranges).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, Result<ObjectMeta>> {
        self.inner.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
        self.inner.copy_opts(from, to, options).await
    }

    async fn rename_opts(&self, from: &Path, to: &Path, options: RenameOptions) -> Result<()> {
        self.inner.rename_opts(from, to, options).await
    }
}

struct Upload {
    writer: Arc<Mutex<Writer>>,
    location: Path,
    next: Option<oneshot::Receiver<()>>,
}

impl fmt::Debug for Upload {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Upload")
            .field("location", &self.location)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl MultipartUpload for Upload {
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        let writer = Arc::clone(&self.writer);
        let location = self.location.clone();
        let (done, next) = oneshot::channel();
        let previous = self.next.replace(next);
        async move {
            // Lance may poll part futures concurrently or out of order. Feed
            // bytes to OpenDAL in submission order; it parallelizes HTTP parts.
            if let Some(previous) = previous {
                let _ = previous.await;
            }
            let result = writer
                .lock()
                .await
                .write(Buffer::from_iter(data))
                .await
                .map_err(|error| upload_error(error, &location));
            drop(done);
            result
        }
        .boxed()
    }

    async fn complete(&mut self) -> Result<PutResult> {
        let metadata = self
            .writer
            .lock()
            .await
            .close()
            .await
            .map_err(|error| upload_error(error, &self.location))?;
        Ok(PutResult {
            e_tag: metadata.etag().map(str::to_owned),
            version: metadata.version().map(str::to_owned),
            extensions: Extensions::new(),
        })
    }

    async fn abort(&mut self) -> Result<()> {
        self.writer
            .lock()
            .await
            .abort()
            .await
            .map_err(|error| upload_error(error, &self.location))
    }
}

fn upload_error(error: opendal::Error, location: &Path) -> object_store::Error {
    use object_store::Error;
    match error.kind() {
        opendal::ErrorKind::NotFound => Error::NotFound {
            path: location.to_string(),
            source: Box::new(error),
        },
        opendal::ErrorKind::Unsupported => Error::NotSupported {
            source: Box::new(error),
        },
        opendal::ErrorKind::AlreadyExists => Error::AlreadyExists {
            path: location.to_string(),
            source: Box::new(error),
        },
        opendal::ErrorKind::ConditionNotMatch => Error::Precondition {
            path: location.to_string(),
            source: Box::new(error),
        },
        kind => Error::Generic {
            store: kind.into_static(),
            source: Box::new(error),
        },
    }
}
