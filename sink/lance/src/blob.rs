use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};

use arrow_array::{Array, ArrayRef, LargeBinaryArray};
use arrow_schema::{DataType, Schema};
use lance::blob::{BlobDescriptor, BlobDescriptorArrayBuilder, DedicatedBlobWriter};
use lance_io::object_store::ObjectStore;
use object_store::path::Path;
use open_arrow_sink_core::{
    BLOB_METADATA_KEY, BlobChunk, BlobHandle, BlobPlacement, BlobWriter, Error, RecordBatch,
    Result, SchemaRef, async_trait,
};

use crate::error::backend;

#[derive(Default)]
pub(crate) struct BlobSession {
    pub failed: AtomicBool,
    pub active: AtomicUsize,
}

struct NativeBlob {
    session: Arc<BlobSession>,
    descriptor: BlobDescriptor,
    path: std::path::PathBuf,
    attached: AtomicBool,
}

impl Drop for NativeBlob {
    fn drop(&mut self) {
        if !self.attached.load(Ordering::Relaxed) {
            let _ = std::fs::remove_file(&self.path);
        }
    }
}

struct NativeBlobWriter {
    inner: Option<DedicatedBlobWriter>,
    session: Arc<BlobSession>,
    path: std::path::PathBuf,
    relative: String,
    finished: bool,
    failed: bool,
}

impl Drop for NativeBlobWriter {
    fn drop(&mut self) {
        self.session.active.fetch_sub(1, Ordering::Relaxed);
        if !self.finished {
            self.session.failed.store(true, Ordering::Relaxed);
            let _ = std::fs::remove_file(&self.path);
        }
    }
}

#[async_trait]
impl BlobWriter for NativeBlobWriter {
    async fn write(&mut self, chunk: BlobChunk) -> Result<()> {
        if self.failed || self.session.failed.load(Ordering::Relaxed) {
            return Err(Error::Unsupported(
                "blob session failed; abort or drop it".into(),
            ));
        }
        self.failed = true;
        if let Err(error) = self.inner.as_mut().unwrap().write(chunk.as_ref()).await {
            self.session.failed.store(true, Ordering::Relaxed);
            return Err(backend(error));
        }
        self.failed = false;
        Ok(())
    }

    async fn finish(mut self: Box<Self>) -> Result<BlobHandle> {
        if self.failed || self.session.failed.load(Ordering::Relaxed) {
            return Err(Error::Unsupported(
                "blob session failed; abort or drop it".into(),
            ));
        }
        let descriptor = self.inner.take().unwrap().finish().await.map_err(backend)?;
        let BlobDescriptor::Dedicated { size, .. } = descriptor else {
            unreachable!()
        };
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
            attached: AtomicBool::new(false),
        }))
    }

    async fn abort(self: Box<Self>) -> Result<()> {
        drop(self);
        Ok(())
    }
}

pub(crate) async fn create_blob(
    root: &std::path::Path,
    session: Arc<BlobSession>,
) -> Result<Box<dyn BlobWriter>> {
    let data_path = root
        .join("_blobs")
        .join(format!("{}.lance", uuid::Uuid::new_v4()));
    let data_path = Path::from_absolute_path(data_path).map_err(backend)?;
    let inner = DedicatedBlobWriter::try_new(ObjectStore::local(), data_path, 1)
        .await
        .map_err(backend)?;
    let path = std::path::PathBuf::from("/").join(inner.path().as_ref());
    let relative = path
        .strip_prefix(root)
        .map_err(backend)?
        .to_str()
        .ok_or_else(|| Error::Unsupported("blob path must be UTF-8".into()))?
        .replace('\\', "/");
    session.active.fetch_add(1, Ordering::Relaxed);
    Ok(Box::new(NativeBlobWriter {
        inner: Some(inner),
        session,
        path,
        relative,
        finished: false,
        failed: false,
    }))
}

pub(crate) fn storage_schema(schema: &SchemaRef) -> Result<SchemaRef> {
    let mut has_blobs = false;
    let fields = schema
        .fields()
        .iter()
        .map(|field| {
            if field.metadata().get(BLOB_METADATA_KEY).map(String::as_str) != Some("true") {
                return Ok(field.clone());
            }
            if field.data_type() != &DataType::LargeBinary {
                return Err(Error::Unsupported(format!(
                    "blob column must be LargeBinary: {}",
                    field.name()
                )));
            }
            has_blobs = true;
            let native = BlobDescriptorArrayBuilder::new(field.name())
                .field()
                .clone();
            let mut metadata = field.metadata().clone();
            metadata.extend(native.metadata().clone());
            Ok(Arc::new(
                native
                    .with_nullable(field.is_nullable())
                    .with_metadata(metadata),
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    let mut metadata = schema.metadata().clone();
    if has_blobs {
        metadata.insert("lance.blob.managed".into(), "true".into());
    }
    Ok(Arc::new(Schema::new_with_metadata(fields, metadata)))
}

pub(crate) fn storage_batch(
    batch: RecordBatch,
    schema: &SchemaRef,
    blobs: Vec<BlobPlacement>,
    session: &Arc<BlobSession>,
) -> Result<RecordBatch> {
    let mut placements = HashMap::new();
    for blob in blobs {
        let Some(field) = batch.schema_ref().fields().get(blob.column) else {
            return Err(Error::Unsupported("blob column out of bounds".into()));
        };
        if blob.row >= batch.num_rows()
            || field.metadata().get(BLOB_METADATA_KEY).map(String::as_str) != Some("true")
        {
            return Err(Error::Unsupported(
                "blob placement must target a blob-marked cell".into(),
            ));
        }
        let array = batch
            .column(blob.column)
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .ok_or_else(|| Error::Unsupported("blob column must be LargeBinary".into()))?;
        if array.is_null(blob.row) || !array.value(blob.row).is_empty() {
            return Err(Error::Unsupported(
                "blob placement must replace an empty non-null payload".into(),
            ));
        }
        let native = blob
            .blob
            .downcast_ref::<NativeBlob>()
            .ok_or_else(|| Error::Unsupported("foreign blob handle".into()))?;
        if !Arc::ptr_eq(&native.session, session) {
            return Err(Error::Unsupported(
                "blob handle belongs to another session".into(),
            ));
        }
        if placements
            .insert((blob.column, blob.row), blob.blob)
            .is_some()
        {
            return Err(Error::Unsupported("duplicate blob placement".into()));
        }
    }
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
        handle
            .downcast_ref::<NativeBlob>()
            .unwrap()
            .attached
            .store(true, Ordering::Relaxed);
    }
    Ok(output)
}
