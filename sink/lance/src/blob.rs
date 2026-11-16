use std::{
    collections::HashSet,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};

use crate::{error::backend, validation};
use arrow_array::{Array, ArrayRef, LargeBinaryArray};
use arrow_schema::Schema;
use lance::blob::{BlobDescriptor, BlobDescriptorArrayBuilder};
use open_arrow_sink_core::{
    BLOB_METADATA_KEY, BlobChunk, BlobHandle, BlobPlacement, BlobWriter, RecordBatch, Result,
    SchemaRef, async_trait,
};
use open_arrow_sink_storage::{Storage, StorageWriter, Url};

pub(crate) struct BlobSession {
    pub failed: AtomicBool,
    io_tracker: Arc<crate::io_metrics::TrackedIo>,
    pub active: AtomicUsize,
    storage: Storage,
    relative_prefix: String,
    directory: Url,
    unattached: Mutex<HashSet<Url>>,
}
impl BlobSession {
    pub fn new(
        storage: Storage,
        dataset: &Url,
        io_tracker: Arc<crate::io_metrics::TrackedIo>,
    ) -> Self {
        let relative_prefix = format!("_blobs/{}/", uuid::Uuid::new_v4());
        let directory = dataset.join(&relative_prefix).unwrap();
        Self {
            failed: AtomicBool::new(false),
            io_tracker,
            active: AtomicUsize::new(0),
            storage,
            relative_prefix,
            directory,
            unattached: Mutex::default(),
        }
    }
    /// All native streams in this namespace belong to this session, including
    /// handles already attached to batches that were never committed.
    pub async fn abort(&self) -> Result<()> {
        self.storage.remove_all(&self.directory).await
    }
    pub async fn cleanup(&self) -> Result<()> {
        let files: Vec<_> = self.unattached.lock().unwrap().drain().collect();
        for url in files {
            self.storage.delete(&url).await?;
        }
        Ok(())
    }
}

pub(crate) struct NativeBlob {
    pub session: Arc<BlobSession>,
    pub descriptor: BlobDescriptor,
    url: Option<Url>,
}

struct NativeBlobWriter {
    inner: Option<StorageWriter>,
    inline_limit: Option<usize>,
    buffer: Vec<u8>,
    session: Arc<BlobSession>,
    url: Url,
    relative: String,
    finished: bool,
    failed: bool,
}
impl Drop for NativeBlobWriter {
    fn drop(&mut self) {
        if !self.finished {
            self.session.failed.store(true, Ordering::Release);
        }
        self.session.active.fetch_sub(1, Ordering::Release);
    }
}
#[async_trait]
impl BlobWriter for NativeBlobWriter {
    async fn write(&mut self, chunk: BlobChunk) -> Result<()> {
        validation::validate_blob_state(self.failed, &self.session)?;
        self.failed = true;
        let result = async {
            if self.inner.is_none()
                && self.inline_limit.is_some_and(|limit| {
                    chunk.as_ref().len() <= limit.saturating_sub(self.buffer.len())
                })
            {
                // Retain at most the configured payload limit, not the source chunk's
                // possibly larger backing allocation. Avoid geometric Vec growth.
                self.buffer.reserve_exact(chunk.as_ref().len());
                self.buffer.extend_from_slice(chunk.as_ref());
                return Ok(());
            }
            if self.inner.is_none() {
                // Track ownership before the first storage await so cancellation
                // cannot leave an untracked object. Small inline blobs never open one.
                self.session
                    .unattached
                    .lock()
                    .unwrap()
                    .insert(self.url.clone());
                self.inner = Some(self.session.storage.writer(&self.url).await?);
                if !self.buffer.is_empty() {
                    let buffered = std::mem::take(&mut self.buffer);
                    self.inner.as_mut().unwrap().write(buffered.into()).await?;
                }
            }
            self.inner
                .as_mut()
                .unwrap()
                .write(chunk.bytes().clone())
                .await
        }
        .await;
        if let Err(error) = result {
            self.session.failed.store(true, Ordering::Release);
            return Err(error);
        }
        self.failed = false;
        Ok(())
    }
    async fn finish(mut self: Box<Self>) -> Result<BlobHandle> {
        validation::validate_blob_state(self.failed, &self.session)?;
        let (descriptor, url) = if let Some(inner) = self.inner.as_mut() {
            let size = inner.close().await?;
            self.session.io_tracker.record_write(
                "streamed_blob",
                object_store::path::Path::from(self.url.path()),
                size,
            );
            (
                BlobDescriptor::Managed {
                    base_id: None,
                    uri: self.relative.clone(),
                    offset: 0,
                    size,
                },
                Some(self.url.clone()),
            )
        } else {
            (
                BlobDescriptor::Inline {
                    data: std::mem::take(&mut self.buffer).into(),
                },
                None,
            )
        };
        self.finished = true;
        Ok(BlobHandle::new(NativeBlob {
            session: self.session.clone(),
            descriptor,
            url,
        }))
    }
    async fn abort(mut self: Box<Self>) -> Result<()> {
        if let Some(inner) = self.inner.as_mut() {
            let upload = inner.abort().await;
            let file = self.session.storage.delete(&self.url).await;
            upload.and(file)
        } else {
            Ok(())
        }
    }
}

pub(crate) async fn create_blob(
    directory: &Url,
    session: Arc<BlobSession>,
    inline_limit: Option<usize>,
) -> Result<Box<dyn BlobWriter>> {
    let relative = format!("{}{}.blob", session.relative_prefix, uuid::Uuid::new_v4());
    let url = directory.join(&relative).unwrap();
    let inner = if inline_limit.is_none() {
        // Preserve the constant-memory dedicated path unless buffering is opted into.
        session.unattached.lock().unwrap().insert(url.clone());
        Some(session.storage.writer(&url).await?)
    } else {
        None
    };
    session.active.fetch_add(1, Ordering::Relaxed);
    Ok(Box::new(NativeBlobWriter {
        inner,
        inline_limit,
        buffer: Vec::new(),
        session,
        url,
        relative,
        finished: false,
        failed: false,
    }))
}

pub(crate) fn storage_schema(schema: &SchemaRef) -> Result<SchemaRef> {
    crate::validation::validate_blob_schema(schema)?;
    if !schema
        .fields()
        .iter()
        .any(|field| field.metadata().get(BLOB_METADATA_KEY).map(String::as_str) == Some("true"))
    {
        return Ok(schema.clone());
    }
    let fields = schema
        .fields()
        .iter()
        .map(|field| {
            if field.metadata().get(BLOB_METADATA_KEY).map(String::as_str) != Some("true") {
                return field.clone();
            }
            let native = BlobDescriptorArrayBuilder::new(field.name())
                .field()
                .clone();
            let mut metadata = field.metadata().clone();
            metadata.extend(native.metadata().clone());
            Arc::new(
                native
                    .with_nullable(field.is_nullable())
                    .with_metadata(metadata),
            )
        })
        .collect::<Vec<_>>();
    let mut metadata = schema.metadata().clone();
    metadata.insert("lance.blob.managed".into(), "true".into());
    Ok(Arc::new(Schema::new_with_metadata(fields, metadata)))
}

pub(crate) fn storage_batch(
    batch: RecordBatch,
    schema: &SchemaRef,
    blobs: Vec<BlobPlacement>,
    session: &Arc<BlobSession>,
) -> Result<RecordBatch> {
    let placements = crate::validation::validate_blob_placements(&batch, blobs, session)?;
    let columns = batch
        .columns()
        .iter()
        .enumerate()
        .zip(schema.fields())
        .map(|((column, array), field)| {
            if array.data_type() == field.data_type() {
                return Ok(array.clone());
            }
            let values = array.as_any().downcast_ref::<LargeBinaryArray>().unwrap();
            let mut builder = BlobDescriptorArrayBuilder::new(field.name());
            for row in 0..values.len() {
                if let Some(handle) = placements.get(&(column, row)) {
                    builder
                        .push(
                            handle
                                .downcast_ref::<NativeBlob>()
                                .unwrap()
                                .descriptor
                                .clone(),
                        )
                        .map_err(backend)?;
                } else if values.is_null(row) {
                    builder.push_null().map_err(backend)?;
                } else {
                    builder
                        .push_inline(values.value(row).to_vec())
                        .map_err(backend)?;
                }
            }
            Ok(builder.finish().map_err(backend)?.array().clone() as ArrayRef)
        })
        .collect::<Result<Vec<_>>>()?;
    let options = arrow_array::RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));
    let output =
        RecordBatch::try_new_with_options(schema.clone(), columns, &options).map_err(backend)?;
    for handle in placements.values() {
        let native = handle.downcast_ref::<NativeBlob>().unwrap();
        if let Some(url) = &native.url {
            session.unattached.lock().unwrap().remove(url);
        }
    }
    Ok(output)
}

/// Lance persists logical blob fields, rather than the prepared descriptor shape.
pub(crate) fn logical_schema(schema: &SchemaRef) -> SchemaRef {
    let fields = schema
        .fields()
        .iter()
        .map(|field| {
            if field.metadata().get(BLOB_METADATA_KEY).map(String::as_str) == Some("true") {
                Arc::new(
                    field.as_ref().clone().with_data_type(
                        lance::blob::blob_field(field.name(), field.is_nullable())
                            .data_type()
                            .clone(),
                    ),
                )
            } else {
                field.clone()
            }
        })
        .collect::<Vec<_>>();
    Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone()))
}
