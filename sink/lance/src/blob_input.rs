//! Materialize blob sources before the SDK makes layout decisions.
use std::{collections::VecDeque, sync::Arc};

use arrow_array::{
    Array, ArrayRef, FixedSizeListArray, LargeBinaryArray, LargeListArray, ListArray, MapArray,
    RecordBatchOptions, StringArray, StructArray,
};
use arrow_buffer::{Buffer, NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow_schema::DataType;
use futures::{StreamExt, future::BoxFuture, stream::FuturesOrdered};
use open_arrow_sink_core::{ExternalBlobMode, IoObservers, RecordBatch, Result, SchemaRef};
use open_arrow_sink_storage::{BlobDownload, BlobDownloader};

use open_arrow_sink_core::Error;

/// Only blob-bearing branches are visited while processing batches.
pub(crate) struct BlobInputPlan {
    schema: SchemaRef,
    columns: Vec<(usize, BlobField)>,
}

struct BlobField {
    field: arrow_schema::FieldRef,
    native: bool,
    children: Vec<(usize, BlobField)>,
}

impl BlobField {
    fn new(field: &arrow_schema::FieldRef) -> Option<Self> {
        let native = crate::blob::is_native_blob(field);
        let children = if native {
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
        (native || !children.is_empty()).then(|| Self {
            field: field.clone(),
            native,
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

// Batch only known top-level payloads. Unknown URI sizes and nested blobs keep
// row admission, so combining rows cannot reject an otherwise admissible row.
fn known_row_bytes(
    batch: &RecordBatch,
    plan: &BlobInputPlan,
    row: usize,
    mode: ExternalBlobMode,
) -> Option<usize> {
    plan.columns
        .iter()
        .try_fold(0_usize, |total, (index, field)| {
            let array = batch.column(*index);
            let bytes = if array.is_null(row) {
                0
            } else if let Some(data) = array.as_any().downcast_ref::<LargeBinaryArray>() {
                data.value(row).len()
            } else if field.native && matches!(array.data_type(), DataType::Struct(_)) {
                let blob = array.as_any().downcast_ref::<StructArray>()?;
                let data = blob
                    .column_by_name("data")?
                    .as_any()
                    .downcast_ref::<LargeBinaryArray>()?;
                let uris = blob.column_by_name("uri")?;
                if data.is_valid(row) {
                    data.value(row).len()
                } else if uris.is_valid(row) && mode == ExternalBlobMode::Ingest {
                    return None;
                } else {
                    0
                }
            } else if field.native && mode == ExternalBlobMode::Reference {
                0
            } else {
                return None;
            };
            total.checked_add(bytes)
        })
}

fn next_range(
    batch: &RecordBatch,
    plan: &BlobInputPlan,
    start: usize,
    mode: ExternalBlobMode,
    limit: usize,
) -> std::ops::Range<usize> {
    let mut end = start;
    let mut bytes = 0_usize;
    while end < batch.num_rows() && end - start < 1024 {
        let Some(next) =
            known_row_bytes(batch, plan, end, mode).and_then(|size| bytes.checked_add(size))
        else {
            break;
        };
        if next > limit {
            break;
        }
        bytes = next;
        end += 1;
    }
    start..end.max(start + 1)
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

/// Bound ready + active groups together, and poll downloads while the SDK's input
/// channel is full. Otherwise downloads could hold every shared HTTP permit while
/// SDK uploads wait for one, with nobody left polling the download bodies.
pub(crate) async fn send_rows(
    batch: &RecordBatch,
    plan: &BlobInputPlan,
    downloader: &BlobDownloader,
    mode: ExternalBlobMode,
    metrics: Option<&IoObservers>,
    recovery: Option<&open_arrow_sink_core::DownloadRecovery>,
    worker: &mut crate::worker::FragmentTask,
) -> Result<()> {
    let mut next = 0;
    let mut skipped = 0;
    let mut active = FuturesOrdered::new();
    let mut ready: VecDeque<RecordBatch> = VecDeque::new();
    loop {
        while active.len() + ready.len() < downloader.in_flight_limit() {
            if next == batch.num_rows() {
                break;
            }
            let range = next_range(batch, plan, next, mode, downloader.max_download_bytes());
            next = range.end;
            active.push_back(materialize_batch(
                batch, range, plan, downloader, mode, metrics, recovery,
            ));
        }
        if active.is_empty() && ready.is_empty() {
            if let Some(config) = recovery {
                config.metrics.skipped(skipped);
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

/// Each batch fits one payload reservation; URI batches contain one input row.
/// Reservations remain attached to the arrays queued at Lance.
async fn materialize_batch(
    batch: &RecordBatch,
    range: std::ops::Range<usize>,
    plan: &BlobInputPlan,
    downloader: &BlobDownloader,
    mode: ExternalBlobMode,
    metrics: Option<&IoObservers>,
    recovery: Option<&open_arrow_sink_core::DownloadRecovery>,
) -> Result<RecordBatch> {
    let schema = &plan.schema;
    let input = crate::blob::logical_batch(batch.slice(range.start, range.len()), schema)?;
    let mut group = downloader.begin().await?;
    let mut columns = input.columns().to_vec();
    let mut failed = false;
    for (index, field_plan) in &plan.columns {
        let field = &field_plan.field;
        let result = materialize(
            input.column(*index),
            field_plan,
            &mut group,
            mode,
            metrics,
            None,
        )
        .await;
        match result {
            Ok(array) => columns[*index] = array,
            Err(error) => {
                let recover = recovery.filter(|config| config.columns.contains(field.name()));
                let source_error = matches!(&error, Error::Backend(source) if source.is::<open_arrow_sink_storage::SourceDownloadError>());
                if let Some(config) = recover.filter(|_| source_error) {
                    config.metrics.failure(!failed);
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
        && recovery.is_some_and(|config| {
            config.policy == open_arrow_sink_core::DownloadFailurePolicy::SkipRow
        })
    {
        return Ok(RecordBatch::new_empty(schema.clone()));
    }
    RecordBatch::try_new_with_options(
        schema.clone(),
        columns,
        &RecordBatchOptions::new().with_row_count(Some(range.len())),
    )
    .map_err(Error::backend)
}

fn materialize<'a>(
    array: &'a ArrayRef,
    plan: &'a BlobField,
    group: &'a mut BlobDownload,
    mode: ExternalBlobMode,
    metrics: Option<&'a IoObservers>,
    parent_nulls: Option<NullBuffer>,
) -> BoxFuture<'a, Result<ArrayRef>> {
    Box::pin(async move {
        let field = &plan.field;
        let nulls = NullBuffer::union(array.nulls(), parent_nulls.as_ref());
        // Lists can contain several logical blobs in one input row. Slice their
        // child range before materializing so a sliced batch cannot retain/read
        // unrelated list elements outside this row's reservation.
        macro_rules! list {
            ($ty:ty, $offset:ty, $child:expr) => {{
                let values = array.as_any().downcast_ref::<$ty>().unwrap();
                let offsets = values.value_offsets();
                let start = offsets[0];
                let end = *offsets.last().unwrap();
                let child = values
                    .values()
                    .slice(start as usize, (end - start) as usize);
                let visibility = child_nulls(
                    nulls.as_ref(),
                    offsets.windows(2).map(|span| (span[1] - span[0]) as usize),
                );
                let child = materialize(
                    &child,
                    &plan.children[0].1,
                    group,
                    mode,
                    metrics,
                    visibility,
                )
                .await?;
                let offsets = OffsetBuffer::new(ScalarBuffer::from(
                    offsets
                        .iter()
                        .map(|offset| offset - start)
                        .collect::<Vec<$offset>>(),
                ));
                return Ok(Arc::new(
                    <$ty>::try_new($child.clone(), offsets, child, values.nulls().cloned())
                        .map_err(Error::backend)?,
                ) as ArrayRef);
            }};
        }
        match field.data_type() {
            DataType::List(child) => list!(ListArray, i32, child),
            DataType::LargeList(child) => list!(LargeListArray, i64, child),
            DataType::FixedSizeList(child, size) => {
                let values = array.as_any().downcast_ref::<FixedSizeListArray>().unwrap();
                let child_values = values.values().clone();
                let visibility = child_nulls(
                    nulls.as_ref(),
                    std::iter::repeat_n(*size as usize, values.len()),
                );
                let child_values = materialize(
                    &child_values,
                    &plan.children[0].1,
                    group,
                    mode,
                    metrics,
                    visibility,
                )
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
                let offsets = values.value_offsets();
                let start = offsets[0];
                let end = *offsets.last().unwrap();
                let entries: ArrayRef = Arc::new(
                    values
                        .entries()
                        .slice(start as usize, (end - start) as usize),
                );
                let visibility = child_nulls(
                    nulls.as_ref(),
                    offsets.windows(2).map(|span| (span[1] - span[0]) as usize),
                );
                let entries = materialize(
                    &entries,
                    &plan.children[0].1,
                    group,
                    mode,
                    metrics,
                    visibility,
                )
                .await?;
                let offsets = OffsetBuffer::new(ScalarBuffer::from(
                    offsets
                        .iter()
                        .map(|offset| offset - start)
                        .collect::<Vec<_>>(),
                ));
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
        if plan.native && values.null_count() == values.len() {
            return Ok(array.clone());
        }
        if plan.native && values.column_by_name("kind").is_none() {
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
                let bytes = if nulls.as_ref().is_some_and(|nulls| nulls.is_null(row)) {
                    None
                } else if data.is_valid(row) {
                    Some(group.buffer(data.value(row))?)
                } else if uris.is_valid(row) && mode == ExternalBlobMode::Ingest {
                    let bytes = group.download(uris.value(row), metrics).await?;
                    let number = |name: &str| {
                        values.column_by_name(name).and_then(|array| {
                            let array =
                                array.as_any().downcast_ref::<arrow_array::UInt64Array>()?;
                            array.is_valid(row).then(|| array.value(row))
                        })
                    };
                    let start = number("position").unwrap_or(0);
                    let end = number("size")
                        .map(|size| start.checked_add(size))
                        .unwrap_or(Some(bytes.len() as u64))
                        .filter(|&end| start <= end && end <= bytes.len() as u64)
                        .ok_or_else(|| {
                            Error::backend(std::io::Error::other(
                                "blob range exceeds downloaded source",
                            ))
                        })?;
                    Some(bytes.slice(start as usize..end as usize))
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
            let columns = fields
                .iter()
                .enumerate()
                .map(|(index, field)| -> Result<ArrayRef> {
                    Ok(match field.name().as_str() {
                        "data" => data.clone(),
                        "uri" => uris.clone(),
                        // Range metadata applies only to preserved URI references.
                        _ => {
                            let validity = arrow_buffer::NullBuffer::from(
                                materialized.iter().map(|&done| !done).collect::<Vec<_>>(),
                            );
                            let original = values.column(index).to_data();
                            arrow_array::make_array(
                                original
                                    .into_builder()
                                    .nulls(arrow_buffer::NullBuffer::union(
                                        values.column(index).nulls(),
                                        Some(&validity),
                                    ))
                                    .build()
                                    .map_err(Error::backend)?,
                            )
                        }
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            return Ok(Arc::new(
                StructArray::try_new(fields.clone(), columns, values.nulls().cloned())
                    .map_err(Error::backend)?,
            ) as ArrayRef);
        }
        let mut columns = values.columns().to_vec();
        for (index, child) in &plan.children {
            columns[*index] = materialize(
                values.column(*index),
                child,
                group,
                mode,
                metrics,
                nulls.clone(),
            )
            .await?;
        }
        Ok(Arc::new(
            StructArray::try_new(fields.clone(), columns, values.nulls().cloned())
                .map_err(Error::backend)?,
        ) as ArrayRef)
    })
}
