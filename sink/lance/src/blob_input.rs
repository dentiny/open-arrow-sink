//! Materialize blob sources before the SDK makes layout decisions.
//!
//! The input plan describes the target schema, whose Blob leaves are logical
//! structs. Source arrays can still contain marked bytes or URL strings:
//! materialize_batch admits a concurrent group, materialize rebuilds its containers,
//! and materialize_blob handles visible payloads/URIs at each Blob leaf.
use std::{collections::VecDeque, sync::Arc};

use arrow_array::{
    Array, ArrayRef, FixedSizeListArray, LargeBinaryArray, LargeListArray, ListArray, MapArray,
    OffsetSizeTrait, RecordBatchOptions, StringArray, StructArray,
};
use arrow_buffer::{Buffer, NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow_cast::cast;
use arrow_schema::DataType;
use futures::{StreamExt, future::BoxFuture, stream::FuturesUnordered};
use open_arrow_sink_core::{
    DownloadRecovery, ExternalBlobMode, IoObservers, RecordBatch, Result, SchemaRef, WriteOptions,
};
use open_arrow_sink_storage::{BlobDownload, BlobDownloader};

use open_arrow_sink_core::Error;

/// Only blob-bearing branches are visited while processing batches.
pub(crate) struct BlobInputPlan {
    schema: SchemaRef,
    columns: Vec<(usize, BlobField)>,
}

struct BlobField {
    field: arrow_schema::FieldRef,
    /// The target field is a Lance Blob leaf, even if its source is bytes or text.
    is_blob: bool,
    children: Vec<(usize, BlobField)>,
}

impl BlobField {
    fn new(field: &arrow_schema::FieldRef) -> Option<Self> {
        let is_blob = crate::blob::is_native_blob(field);
        let children = if is_blob {
            Vec::new()
        } else {
            match field.data_type() {
                DataType::Struct(fields) => fields
                    .iter()
                    .enumerate()
                    .filter_map(|(index, field)| Self::new(field).map(|plan| (index, plan)))
                    .collect(),
                DataType::List(child)
                | DataType::LargeList(child)
                | DataType::FixedSizeList(child, _)
                | DataType::Map(child, _) => Self::new(child)
                    .map(|plan| vec![(0, plan)])
                    .unwrap_or_default(),
                _ => Vec::new(),
            }
        };
        (is_blob || !children.is_empty()).then(|| Self {
            field: field.clone(),
            is_blob,
            children,
        })
    }
}

impl BlobInputPlan {
    pub fn new(schema: SchemaRef) -> Self {
        let columns = schema
            .fields()
            .iter()
            .enumerate()
            .filter_map(|(index, field)| BlobField::new(field).map(|plan| (index, plan)))
            .collect();
        Self { schema, columns }
    }

    pub fn is_empty(&self) -> bool {
        self.columns.is_empty()
    }
}

// Batch known top-level payloads. URI and nested blob inputs keep row admission
// so download recovery can handle each input row independently.
fn can_batch_row(
    batch: &RecordBatch,
    plan: &BlobInputPlan,
    row: usize,
    mode: ExternalBlobMode,
) -> bool {
    plan.columns.iter().all(|(index, field)| {
        let array = batch.column(*index);
        if array.is_null(row) || array.as_any().is::<LargeBinaryArray>() {
            true
        } else if field.is_blob && matches!(array.data_type(), DataType::Struct(_)) {
            let blob = array.as_any().downcast_ref::<StructArray>().unwrap();
            let Some(data) = blob
                .column_by_name("data")
                .and_then(|array| array.as_any().downcast_ref::<LargeBinaryArray>())
            else {
                return false;
            };
            let Some(uris) = blob.column_by_name("uri") else {
                return false;
            };
            data.is_valid(row) || uris.is_null(row) || mode == ExternalBlobMode::Reference
        } else {
            field.is_blob && mode == ExternalBlobMode::Reference
        }
    })
}

fn next_range(
    batch: &RecordBatch,
    plan: &BlobInputPlan,
    start: usize,
    mode: ExternalBlobMode,
) -> std::ops::Range<usize> {
    let mut end = start;
    while end < batch.num_rows() && end - start < 1024 {
        if !can_batch_row(batch, plan, end, mode) {
            break;
        }
        end += 1;
    }
    start..end.max(start + 1)
}

/// Restrict nested materialization to the visible child span and rebase its offsets.
fn rebase_offsets<O: OffsetSizeTrait>(
    offsets: &OffsetBuffer<O>,
) -> (std::ops::Range<usize>, OffsetBuffer<O>) {
    let start = offsets[0];
    let end = *offsets.last().unwrap();
    let range = start.as_usize()..end.as_usize();
    let offsets = OffsetBuffer::new(ScalarBuffer::from(
        offsets
            .iter()
            .map(|offset| *offset - start)
            .collect::<Vec<O>>(),
    ));
    (range, offsets)
}

fn child_nulls(
    nulls: Option<&NullBuffer>,
    lengths: impl Iterator<Item = usize>,
) -> Option<NullBuffer> {
    nulls.filter(|nulls| nulls.null_count() != 0).map(|nulls| {
        NullBuffer::from_iter(
            lengths
                .enumerate()
                .flat_map(|(row, len)| std::iter::repeat_n(nulls.is_valid(row), len)),
        )
    })
}

/// Logical Blob references may address a byte range within the downloaded object.
/// Validate it before narrowing the SDK's u64 positions to buffer indices.
fn blob_source_range(
    values: &StructArray,
    row: usize,
    source_len: usize,
) -> Result<std::ops::Range<usize>> {
    let number = |name: &str| {
        values.column_by_name(name).and_then(|array| {
            let array = array.as_any().downcast_ref::<arrow_array::UInt64Array>()?;
            array.is_valid(row).then(|| array.value(row))
        })
    };
    let start = number("position").unwrap_or(0);
    let end = number("size")
        .map(|size| start.checked_add(size))
        .unwrap_or(Some(source_len as u64))
        .filter(|&end| start <= end && end <= source_len as u64)
        .ok_or_else(|| {
            Error::backend(std::io::Error::other(
                "blob range exceeds downloaded source",
            ))
        })?;
    Ok(start as usize..end as usize)
}

/// Per-writer blob download policy shared by batch and recursive array conversion.
pub(crate) struct BlobMaterializer {
    downloader: Arc<BlobDownloader>,
    mode: ExternalBlobMode,
    metrics: Option<IoObservers>,
    recovery: Option<DownloadRecovery>,
    pub(crate) downloads: open_arrow_sink_core::DownloadMetrics,
}

impl BlobMaterializer {
    pub fn new(downloader: Arc<BlobDownloader>, options: &WriteOptions) -> Self {
        Self {
            downloader,
            mode: options
                .blob_handle
                .as_ref()
                .map(|config| config.external_blob_mode)
                .unwrap_or_default(),
            metrics: options.io_metrics.clone(),
            recovery: options.download_recovery.clone(),
            downloads: Default::default(),
        }
    }

    /// Bound ready + active groups together, and poll downloads while the SDK's input
    /// channel is full. Otherwise downloads could hold every shared HTTP permit while
    /// SDK uploads wait for one, with nobody left polling the download bodies.
    pub(crate) async fn send_rows(
        &self,
        batch: &RecordBatch,
        plan: &BlobInputPlan,
        worker: &mut crate::worker::FragmentTask,
    ) -> Result<()> {
        let mut next = 0;
        let mut skipped = 0;
        // Lance sessions only support Ordering::Unordered. Forward completed groups
        // immediately so one slow source does not hold up the SDK's input queue.
        let mut active = FuturesUnordered::new();
        let mut ready: VecDeque<RecordBatch> = VecDeque::new();
        loop {
            while active.len() + ready.len() < self.downloader.in_flight_limit() {
                if next == batch.num_rows() {
                    break;
                }
                let range = next_range(batch, plan, next, self.mode);
                next = range.end;
                active.push(self.materialize_batch(batch, range, plan));
            }
            if active.is_empty() && ready.is_empty() {
                if let Some(config) = self.recovery.as_ref() {
                    config.metrics.skipped(skipped);
                    self.downloads.skipped(skipped);
                }
                return Ok(());
            }
            tokio::select! {
                row = active.next(), if !active.is_empty() => {
                    let row = row.unwrap()?;
                    if row.num_rows() == 0 { skipped += 1; } else { ready.push_back(row); }
                }
                result = async { worker.write(ready.front().unwrap().clone()).await }, if !ready.is_empty() => {
                    result?;
                    ready.pop_front();
                }
            }
        }
    }

    /// URI batches contain one input row for independent download recovery.
    async fn materialize_batch(
        &self,
        batch: &RecordBatch,
        range: std::ops::Range<usize>,
        plan: &BlobInputPlan,
    ) -> Result<RecordBatch> {
        let schema = &plan.schema;
        let input = batch.slice(range.start, range.len());
        let mut group = self.downloader.begin().await?;
        let mut columns = input.columns().to_vec();
        let mut failed = false;
        for (index, field_plan) in &plan.columns {
            let field = &field_plan.field;
            let result = self
                .materialize(input.column(*index), field_plan, &mut group, None)
                .await;
            match result {
                Ok(array) => columns[*index] = array,
                Err(error) => {
                    let recover = self
                        .recovery
                        .as_ref()
                        .filter(|config| config.columns.contains(field.name()));
                    let source_error = matches!(&error, Error::Backend(source) if source.is::<open_arrow_sink_storage::SourceDownloadError>());
                    if let Some(config) = recover.filter(|_| source_error) {
                        config.metrics.failure(!failed);
                        self.downloads.failure(!failed);
                        failed = true;
                        if config.policy != open_arrow_sink_core::DownloadFailurePolicy::Fail {
                            columns[*index] =
                                arrow_array::new_null_array(field.data_type(), range.len());
                            continue;
                        }
                    }
                    return Err(crate::error::blob_input(field.name(), range.start, error));
                }
            }
        }
        if failed
            && self.recovery.as_ref().is_some_and(|config| {
                config.policy == open_arrow_sink_core::DownloadFailurePolicy::SkipRow
            })
        {
            return Ok(RecordBatch::new_empty(schema.clone()));
        }
        RecordBatch::try_new_with_options(
            schema.clone(),
            columns,
            &RecordBatchOptions::new()
                .with_match_field_names(false)
                .with_row_count(Some(range.len())),
        )
        .map_err(Error::backend)
    }

    fn materialize<'a>(
        &'a self,
        array: &'a ArrayRef,
        plan: &'a BlobField,
        group: &'a mut BlobDownload,
        parent_nulls: Option<NullBuffer>,
    ) -> BoxFuture<'a, Result<ArrayRef>> {
        Box::pin(async move {
            let field = &plan.field;
            // Convert at the leaf, after list slicing has selected this group's
            // child range. Container traversal also carries ancestor nulls down.
            let array = if plan.is_blob {
                crate::blob::convert_blob_leaf(array, field)?
            } else {
                array.clone()
            };
            let nulls = NullBuffer::union(array.nulls(), parent_nulls.as_ref());
            // Lists can contain several logical blobs in one input row. Slice their
            // child range before materializing so a sliced batch cannot retain/read
            // unrelated list elements outside this row's child span.
            macro_rules! list {
                ($ty:ty, $child:expr) => {{
                    let values = array.as_any().downcast_ref::<$ty>().unwrap();
                    let (range, offsets) = rebase_offsets(values.offsets());
                    let child = values.values().slice(range.start, range.len());
                    let visibility = child_nulls(
                        nulls.as_ref(),
                        offsets.windows(2).map(|span| (span[1] - span[0]) as usize),
                    );
                    let child = self
                        .materialize(&child, &plan.children[0].1, group, visibility)
                        .await?;
                    return Ok(Arc::new(
                        <$ty>::try_new($child.clone(), offsets, child, values.nulls().cloned())
                            .map_err(Error::backend)?,
                    ) as ArrayRef);
                }};
            }
            match field.data_type() {
                DataType::List(child) => list!(ListArray, child),
                DataType::LargeList(child) => list!(LargeListArray, child),
                DataType::FixedSizeList(child, size) => {
                    let values = array.as_any().downcast_ref::<FixedSizeListArray>().unwrap();
                    let child_values = values.values().clone();
                    let visibility = child_nulls(
                        nulls.as_ref(),
                        std::iter::repeat_n(*size as usize, values.len()),
                    );
                    let child_values = self
                        .materialize(&child_values, &plan.children[0].1, group, visibility)
                        .await?;
                    return Ok(Arc::new(
                        FixedSizeListArray::try_new(
                            child.clone(),
                            *size,
                            child_values,
                            values.nulls().cloned(),
                        )
                        .map_err(Error::backend)?,
                    ) as ArrayRef);
                }
                DataType::Map(child, ordered) => {
                    let values = array.as_any().downcast_ref::<MapArray>().unwrap();
                    let (range, offsets) = rebase_offsets(values.offsets());
                    let entries: ArrayRef =
                        Arc::new(values.entries().slice(range.start, range.len()));
                    let visibility = child_nulls(
                        nulls.as_ref(),
                        offsets.windows(2).map(|span| (span[1] - span[0]) as usize),
                    );
                    let entries = self
                        .materialize(&entries, &plan.children[0].1, group, visibility)
                        .await?;
                    return Ok(Arc::new(
                        MapArray::try_new(
                            child.clone(),
                            offsets,
                            entries
                                .as_any()
                                .downcast_ref::<StructArray>()
                                .unwrap()
                                .clone(),
                            values.nulls().cloned(),
                            *ordered,
                        )
                        .map_err(Error::backend)?,
                    ) as ArrayRef);
                }
                _ => {}
            }
            let DataType::Struct(fields) = field.data_type() else {
                return Ok(array.clone());
            };
            let values = array.as_any().downcast_ref::<StructArray>().unwrap();
            if plan.is_blob && values.null_count() == values.len() {
                return Ok(array.clone());
            }
            if plan.is_blob && values.column_by_name("kind").is_none() {
                return self
                    .materialize_blob(values, field, group, nulls.as_ref())
                    .await;
            }
            // Ordinary structs visit only Blob-bearing children. Cast the remaining
            // children to align any nested compression metadata with the target.
            let mut columns = values.columns().to_vec();
            for (index, child) in &plan.children {
                columns[*index] = self
                    .materialize(values.column(*index), child, group, nulls.clone())
                    .await?;
            }
            for (array, field) in columns.iter_mut().zip(fields) {
                *array = cast(array, field.data_type()).map_err(Error::backend)?;
            }
            Ok(Arc::new(
                StructArray::try_new(fields.clone(), columns, values.nulls().cloned())
                    .map_err(Error::backend)?,
            ) as ArrayRef)
        })
    }

    /// Handle one logical Blob array after container traversal has established
    /// visibility. Hidden values must never trigger downloads. Ingest replaces
    /// URIs with payload buffers; Reference mode keeps URI/range fields.
    async fn materialize_blob(
        &self,
        values: &StructArray,
        field: &arrow_schema::Field,
        group: &mut BlobDownload,
        nulls: Option<&NullBuffer>,
    ) -> Result<ArrayRef> {
        let DataType::Struct(fields) = field.data_type() else {
            unreachable!("logical Blob fields are structs");
        };
        let malformed = || {
            Error::backend(std::io::Error::other(format!(
                "invalid logical blob field {}",
                field.name()
            )))
        };
        let data = values
            .column_by_name("data")
            .and_then(|array| array.as_any().downcast_ref::<LargeBinaryArray>())
            .ok_or_else(malformed)?;
        let uris = values
            .column_by_name("uri")
            .and_then(|array| array.as_any().downcast_ref::<StringArray>())
            .ok_or_else(malformed)?;
        let mut payload = Vec::new();
        let mut offsets = vec![0_i64];
        let mut output_uris = Vec::with_capacity(values.len());
        let mut data_validity = Vec::with_capacity(values.len());
        let mut materialized = Vec::with_capacity(values.len());
        for row in 0..values.len() {
            let mut uri = None;
            let bytes = if nulls.is_some_and(|nulls| nulls.is_null(row)) {
                None
            } else if data.is_valid(row) {
                Some(group.buffer(data.value(row))?)
            } else if uris.is_valid(row) && self.mode == ExternalBlobMode::Ingest {
                let bytes = group
                    .download(uris.value(row), self.metrics.as_ref())
                    .await?;
                Some(bytes.slice(blob_source_range(values, row, bytes.len())?))
            } else {
                if uris.is_valid(row) {
                    uri = Some(uris.value(row).to_owned());
                }
                None
            };
            data_validity.push(bytes.is_some());
            materialized.push(bytes.is_some());
            if let Some(bytes) = bytes {
                payload
                    .try_reserve_exact(bytes.len())
                    .map_err(Error::backend)?;
                payload.extend_from_slice(&bytes);
            }
            offsets.push(i64::try_from(payload.len()).map_err(Error::backend)?);
            output_uris.push(uri);
        }
        let payload = group.finish_buffer(payload)?;
        let data: ArrayRef = Arc::new(LargeBinaryArray::new(
            OffsetBuffer::new(ScalarBuffer::from(offsets)),
            Buffer::from(payload),
            Some(arrow_buffer::NullBuffer::from(data_validity)),
        ));
        let uris: ArrayRef = Arc::new(StringArray::from(output_uris));
        crate::blob::with_materialized_payloads(values, fields, data, uris, &materialized)
    }
}
