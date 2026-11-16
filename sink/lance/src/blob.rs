use std::{
    collections::HashSet,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};

use arrow_array::{Array, ArrayRef, LargeBinaryArray};
use lance::blob::{BlobDescriptor, BlobDescriptorArrayBuilder};
use open_arrow_sink_core::{
    BlobChunk, BlobHandle, BlobPlacement, BlobWriter, Error, Poison, RecordBatch, Result,
    SchemaRef, async_trait, is_blob_field,
};
use open_arrow_sink_storage::{ObjectPath, Storage, StorageWriter};

pub(crate) struct BlobSession {
    active: AtomicUsize,
    failed: AtomicBool,
    io_tracker: Arc<crate::io_metrics::TrackedIo>,
    storage: Storage,
    relative_prefix: String,
    directory: ObjectPath,
    unattached: Mutex<HashSet<ObjectPath>>,
}
impl BlobSession {
    pub fn new(
        storage: Storage,
        dataset: &ObjectPath,
        io_tracker: Arc<crate::io_metrics::TrackedIo>,
    ) -> Self {
        let relative_prefix = format!("_blobs/{}/", uuid::Uuid::new_v4());
        let directory = dataset.join(&relative_prefix).unwrap();
        Self {
            active: AtomicUsize::new(0),
            failed: AtomicBool::new(false),
            io_tracker,
            storage,
            relative_prefix,
            directory,
            unattached: Mutex::default(),
        }
    }
    pub fn check(&self) -> Result<()> {
        if self.failed.load(Ordering::Acquire) {
            return Err(Error::Poisoned);
        }
        Ok(())
    }

    pub fn fail(&self) {
        self.failed.store(true, Ordering::Release);
    }

    pub fn ensure_idle(&self) -> Result<()> {
        self.check()?;
        if self.active.load(Ordering::Acquire) != 0 {
            return Err(crate::error::unfinished_blobs());
        }
        Ok(())
    }

    pub async fn create_blob(
        self: &Arc<Self>,
        directory: &ObjectPath,
    ) -> Result<Box<dyn BlobWriter>> {
        self.check()?;
        let relative = format!("{}{}.blob", self.relative_prefix, uuid::Uuid::new_v4());
        let path = directory.join(&relative)?;
        // Retain ownership before opening: cancellation can leave an object behind.
        self.unattached.lock().unwrap().insert(path.clone());
        let inner = self.storage.writer(&path).await?;
        self.active.fetch_add(1, Ordering::Relaxed);
        Ok(Box::new(NativeBlobWriter {
            inner,
            session: self.clone(),
            path,
            relative,
            finished: false,
            state: Poison::default(),
        }))
    }

    fn attach(&self, blob: &NativeBlob) {
        self.unattached.lock().unwrap().remove(&blob.path);
    }

    fn release_writer(&self, finished: bool) {
        if !finished {
            self.fail();
        }
        self.active.fetch_sub(1, Ordering::Release);
    }

    /// All native streams in this namespace belong to this session, including
    /// handles already attached to batches that were never committed.
    pub async fn abort(&self) -> Result<()> {
        self.storage.remove_all(&self.directory).await
    }
    pub async fn cleanup(&self) -> Result<()> {
        let files: Vec<_> = self.unattached.lock().unwrap().drain().collect();
        for path in files {
            self.storage.delete(&path).await?;
        }
        Ok(())
    }
}

pub(crate) struct NativeBlob {
    pub session: Arc<BlobSession>,
    pub descriptor: BlobDescriptor,
    path: ObjectPath,
}

struct NativeBlobWriter {
    inner: StorageWriter,
    session: Arc<BlobSession>,
    path: ObjectPath,
    relative: String,
    finished: bool,
    state: Poison,
}
impl Drop for NativeBlobWriter {
    fn drop(&mut self) {
        self.session.release_writer(self.finished);
    }
}
#[async_trait]
impl BlobWriter for NativeBlobWriter {
    async fn write(&mut self, chunk: BlobChunk) -> Result<()> {
        self.state.check()?;
        self.session.check()?;
        self.state.poison();
        if let Err(error) = self.inner.write(chunk.bytes().clone()).await {
            self.session.fail();
            return Err(error);
        }
        self.state.complete();
        Ok(())
    }
    async fn finish(mut self: Box<Self>) -> Result<BlobHandle> {
        self.state.check()?;
        self.session.check()?;
        let size = self.inner.close().await?;
        self.session.io_tracker.record_write(
            "streamed_blob",
            object_store::path::Path::from(self.path.as_str()),
            size,
        );
        self.finished = true;
        Ok(BlobHandle::new(NativeBlob {
            session: self.session.clone(),
            descriptor: BlobDescriptor::Managed {
                base_id: None,
                uri: self.relative.clone(),
                offset: 0,
                size,
            },
            path: self.path.clone(),
        }))
    }
    async fn abort(mut self: Box<Self>) -> Result<()> {
        let upload = self.inner.abort().await;
        let file = self.session.storage.delete(&self.path).await;
        upload.and(file)
    }
}

/// Marked binary columns require the session-owned streaming blob writer.
pub(crate) fn requires_streaming_writer(schema: &SchemaRef) -> bool {
    schema.fields().iter().any(|field| is_blob_field(field))
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
            if !is_blob_field(field) {
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
                        .map_err(Error::backend)?;
                } else if values.is_null(row) {
                    builder.push_null().map_err(Error::backend)?;
                } else {
                    builder
                        .push_inline(values.value(row).to_vec())
                        .map_err(Error::backend)?;
                }
            }
            Ok(builder.finish().map_err(Error::backend)?.array().clone() as ArrayRef)
        })
        .collect::<Result<Vec<_>>>()?;
    // Compression can change nested field metadata while reusing column buffers.
    // Match the metadata-only conversion used by ordinary writers.
    let options = arrow_array::RecordBatchOptions::new()
        .with_match_field_names(false)
        .with_row_count(Some(batch.num_rows()));
    let output = RecordBatch::try_new_with_options(schema.clone(), columns, &options)
        .map_err(Error::backend)?;
    for handle in placements.values() {
        let native = handle.downcast_ref::<NativeBlob>().unwrap();
        session.attach(native);
    }
    Ok(output)
}
