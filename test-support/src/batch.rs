use std::sync::Arc;

use arrow_array::{ArrayRef, Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef};

pub fn int64_schema(column: &str, nullable: bool) -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new(
        column,
        DataType::Int64,
        nullable,
    )]))
}

pub fn int64_batch(column: &str, values: Vec<Option<i64>>) -> RecordBatch {
    RecordBatch::try_from_iter([(column, Arc::new(Int64Array::from(values)) as ArrayRef)]).unwrap()
}

/// Sort rows by the supplied key columns, retaining nulls, metadata and duplicates.
/// Include tie-breaker columns when equal keys can have different row values.
pub fn sort_batch(
    batch: &RecordBatch,
    keys: &[&str],
) -> Result<RecordBatch, arrow_schema::ArrowError> {
    use arrow_ord::sort::{SortColumn, lexsort_to_indices};
    let columns = keys
        .iter()
        .map(|name| {
            Ok(SortColumn {
                values: batch.column(batch.schema().index_of(name)?).clone(),
                options: None,
            })
        })
        .collect::<Result<Vec<_>, arrow_schema::ArrowError>>()?;
    let indices = lexsort_to_indices(&columns, None)?;
    arrow_select::take::take_record_batch(batch, &indices)
}

/// Concatenate scan batches before comparing unordered output, including empty scans.
pub fn concat_sorted(
    schema: &SchemaRef,
    batches: &[RecordBatch],
    keys: &[&str],
) -> Result<RecordBatch, arrow_schema::ArrowError> {
    let batch = arrow_select::concat::concat_batches(schema, batches)?;
    sort_batch(&batch, keys)
}
