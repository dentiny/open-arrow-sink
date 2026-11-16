//! Logical blob input conversion. Layout and object ownership belong to Lance.
use arrow_array::{Array, ArrayRef, LargeBinaryArray, StringArray, StructArray};
use open_arrow_sink_core::{Error, RecordBatch, Result, SchemaRef, is_blob_field};
use std::sync::Arc;

pub(crate) fn is_native_blob(field: &arrow_schema::Field) -> bool {
    field
        .metadata()
        .get("ARROW:extension:name")
        .map(String::as_str)
        == Some("lance.blob.v2")
}

/// Logical input reuses the binary buffers, retaining download reservations until
/// Lance consumes them. This is the same shape produced by BlobArrayBuilder's
/// push_bytes, without another payload copy at the adapter boundary.
pub(crate) fn logical_batch(batch: RecordBatch, schema: &SchemaRef) -> Result<RecordBatch> {
    let columns = batch
        .columns()
        .iter()
        .zip(batch.schema().fields())
        .zip(schema.fields())
        .map(|((array, source), target)| {
            if !is_blob_field(source) {
                return array.clone();
            }
            let values = array.as_any().downcast_ref::<LargeBinaryArray>().unwrap();
            let arrow_schema::DataType::Struct(fields) = target.data_type() else {
                unreachable!("logical blob field is a struct");
            };
            Arc::new(StructArray::new(
                fields.clone(),
                vec![array.clone(), Arc::new(StringArray::new_null(array.len()))],
                values.nulls().cloned(),
            )) as ArrayRef
        })
        .collect();
    let options = arrow_array::RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));
    RecordBatch::try_new_with_options(schema.clone(), columns, &options).map_err(Error::backend)
}
