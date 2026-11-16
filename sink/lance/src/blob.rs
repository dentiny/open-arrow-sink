use std::{
    collections::HashSet,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};

use crate::{error::backend, validation};
use arrow_array::{Array, ArrayRef, LargeBinaryArray};
use arrow_schema::{Field, Schema};
use lance::blob::{BlobDescriptor, BlobDescriptorArrayBuilder};
use open_arrow_sink_core::{
    BlobChunk, BlobHandle, BlobPlacement, BlobWriter, RecordBatch, Result, SchemaRef, async_trait,
    is_blob_marked,
};
use open_arrow_sink_storage::{Storage, StorageWriter, Url};

/// Native logical and prepared descriptor fields carry the same extension name.
pub(crate) fn is_native_blob(field: &Field) -> bool {
    field
        .metadata()
        .get("ARROW:extension:name")
        .map(String::as_str)
        == Some("lance.blob.v2")
}

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
    url: Url,
}

struct NativeBlobWriter {
    inner: StorageWriter,
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
        if let Err(error) = self.inner.write(chunk.bytes().clone()).await {
            self.session.failed.store(true, Ordering::Release);
            return Err(error);
        }
        self.failed = false;
        Ok(())
    }
    async fn finish(mut self: Box<Self>) -> Result<BlobHandle> {
        validation::validate_blob_state(self.failed, &self.session)?;
        let size = self.inner.close().await?;
        self.session.io_tracker.record_write(
            "streamed_blob",
            object_store::path::Path::from(self.url.path()),
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
            url: self.url.clone(),
        }))
    }
    async fn abort(mut self: Box<Self>) -> Result<()> {
        let upload = self.inner.abort().await;
        let file = self.session.storage.delete(&self.url).await;
        upload.and(file)
    }
}

pub(crate) async fn create_blob(
    directory: &Url,
    session: Arc<BlobSession>,
) -> Result<Box<dyn BlobWriter>> {
    let relative = format!("{}{}.blob", session.relative_prefix, uuid::Uuid::new_v4());
    let url = directory.join(&relative).unwrap();
    // Retain ownership before opening: cancellation can leave an object behind.
    session.unattached.lock().unwrap().insert(url.clone());
    let inner = session.storage.writer(&url).await?;
    session.active.fetch_add(1, Ordering::Relaxed);
    Ok(Box::new(NativeBlobWriter {
        inner,
        session,
        url,
        relative,
        finished: false,
        failed: false,
    }))
}

/// Marked binary columns require the session-owned streaming blob writer.
pub(crate) fn requires_streaming_writer(schema: &SchemaRef) -> bool {
    schema.fields().iter().any(|field| is_blob_marked(field))
}

pub(crate) fn storage_schema(schema: &SchemaRef) -> Result<SchemaRef> {
    crate::validation::validate_blob_schema(schema)?;
    if !requires_streaming_writer(schema) {
        return Ok(schema.clone());
    }
    let fields = schema
        .fields()
        .iter()
        .map(|field| {
            if !is_blob_marked(field) {
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
            if !is_blob_marked(field) {
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
    // Encoding changes field metadata, including nested fields, while reusing
    // unselected columns' buffers. Match the metadata-only writer conversion.
    let options = arrow_array::RecordBatchOptions::new()
        .with_match_field_names(false)
        .with_row_count(Some(batch.num_rows()));
    let output =
        RecordBatch::try_new_with_options(schema.clone(), columns, &options).map_err(backend)?;
    for handle in placements.values() {
        let native = handle.downcast_ref::<NativeBlob>().unwrap();
        session.unattached.lock().unwrap().remove(&native.url);
    }
    Ok(output)
}

/// Lance persists logical blob fields, rather than the prepared descriptor shape.
pub(crate) fn logical_schema(schema: &SchemaRef) -> SchemaRef {
    let fields = schema
        .fields()
        .iter()
        .map(|field| {
            if is_blob_marked(field) {
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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::DataType;

    #[test]
    fn native_blob_recognition_matches_logical_and_descriptor_fields() {
        assert!(is_native_blob(&lance::blob::blob_field("blob", true)));
        assert!(is_native_blob(
            BlobDescriptorArrayBuilder::new("blob").field()
        ));
        let field = Field::new("blob", DataType::LargeBinary, true);
        assert!(!is_native_blob(&field));
        for extension in ["", "other.blob"] {
            assert!(!is_native_blob(&field.clone().with_metadata(
                [("ARROW:extension:name".into(), extension.into())].into()
            )));
        }
    }
}
