//! Materialize blob sources before the SDK makes layout decisions.
use std::{collections::VecDeque, sync::Arc};

use arrow_array::{
    Array, ArrayRef, FixedSizeListArray, LargeBinaryArray, LargeListArray, ListArray, MapArray,
    RecordBatchOptions, StringArray, StructArray,
};
use arrow_buffer::{Buffer, OffsetBuffer, ScalarBuffer};
use arrow_schema::{DataType, Field};
use futures::{StreamExt, future::BoxFuture, stream::FuturesOrdered};
use open_arrow_sink_core::{ExternalBlobMode, IoMetrics, RecordBatch, Result, SchemaRef};
use open_arrow_sink_storage::{BlobDownload, BlobDownloader};

use open_arrow_sink_core::Error;

pub(crate) fn has_blobs(schema: &SchemaRef) -> bool {
    fn contains(field: &Field) -> bool {
        crate::blob::is_native_blob(field)
            || match field.data_type() {
                DataType::Struct(fields) => fields.iter().any(|field| contains(field)),
                DataType::List(field)
                | DataType::LargeList(field)
                | DataType::FixedSizeList(field, _)
                | DataType::Map(field, _) => contains(field),
                _ => false,
            }
    }
    schema.fields().iter().any(|field| contains(field))
}

/// Bound ready + active rows together, and poll downloads while the SDK's input
/// channel is full. Otherwise downloads could hold every shared HTTP permit while
/// SDK uploads wait for one, with nobody left polling the download bodies.
pub(crate) async fn send_rows(
    batch: &RecordBatch,
    schema: &SchemaRef,
    downloader: &BlobDownloader,
    mode: ExternalBlobMode,
    metrics: Option<&IoMetrics>,
    worker: &mut crate::worker::FragmentWorker,
) -> Result<()> {
    let mut rows = 0..batch.num_rows();
    let mut active = FuturesOrdered::new();
    let mut ready: VecDeque<RecordBatch> = VecDeque::new();
    loop {
        while active.len() + ready.len() < downloader.in_flight_limit() {
            let Some(row) = rows.next() else {
                break;
            };
            active.push_back(materialize_row(
                batch, row, schema, downloader, mode, metrics,
            ));
        }
        if active.is_empty() && ready.is_empty() {
            return Ok(());
        }
        tokio::select! {
            row = active.next(), if !active.is_empty() => {
                ready.push_back(row.unwrap()?);
            }
            result = async { worker.write(ready.front().unwrap().clone()).await }, if !ready.is_empty() => {
                result?;
                ready.pop_front();
            }
        }
    }
}

/// Process one row at a time so input batch length cannot multiply full-blob
/// allocations. Reservations remain attached to the arrays queued at Lance.
pub(crate) async fn materialize_row(
    batch: &RecordBatch,
    row: usize,
    schema: &SchemaRef,
    downloader: &BlobDownloader,
    mode: ExternalBlobMode,
    metrics: Option<&IoMetrics>,
) -> Result<RecordBatch> {
    let input = crate::blob::logical_batch(batch.slice(row, 1), schema)?;
    let mut group = downloader.begin().await?;
    let mut columns = Vec::with_capacity(input.num_columns());
    for (array, field) in input.columns().iter().zip(schema.fields()) {
        columns.push(
            materialize(array, field, &mut group, mode, metrics)
                .await
                .map_err(|error| crate::error::blob_input(field.name(), row, error))?,
        );
    }
    RecordBatch::try_new_with_options(
        schema.clone(),
        columns,
        &RecordBatchOptions::new().with_row_count(Some(1)),
    )
    .map_err(Error::backend)
}

fn materialize<'a>(
    array: &'a ArrayRef,
    field: &'a Field,
    group: &'a mut BlobDownload,
    mode: ExternalBlobMode,
    metrics: Option<&'a IoMetrics>,
) -> BoxFuture<'a, Result<ArrayRef>> {
    Box::pin(async move {
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
                let child = materialize(&child, $child, group, mode, metrics).await?;
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
                let child_values = materialize(&child_values, child, group, mode, metrics).await?;
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
                let entries = materialize(&entries, child, group, mode, metrics).await?;
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
        if crate::blob::is_native_blob(field) && values.null_count() == values.len() {
            return Ok(array.clone());
        }
        if crate::blob::is_native_blob(field) && values.column_by_name("kind").is_none() {
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
                let bytes = if values.is_null(row) {
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
        let mut columns = Vec::with_capacity(fields.len());
        for (array, field) in values.columns().iter().zip(fields) {
            columns.push(materialize(array, field, group, mode, metrics).await?);
        }
        Ok(Arc::new(
            StructArray::try_new(fields.clone(), columns, values.nulls().cloned())
                .map_err(Error::backend)?,
        ) as ArrayRef)
    })
}
