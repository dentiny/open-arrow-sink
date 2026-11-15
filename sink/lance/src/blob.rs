use std::sync::Arc;

use arrow_array::{Array, ArrayRef, LargeBinaryArray, StringArray, StructArray};
use arrow_schema::{DataType, Schema};
use lance::blob::blob_field;
use open_arrow_sink_core::{BLOB_METADATA_KEY, RecordBatch, Result, SchemaRef};

use crate::{error::backend, validation::validate_blob_schema};

/// Keep the public writer schema stable while giving Lance native blob v2 fields.
pub(crate) fn storage_schema(schema: &SchemaRef) -> Result<SchemaRef> {
    validate_blob_schema(schema)?;
    let fields = schema
        .fields()
        .iter()
        .map(|field| {
            if field.metadata().get(BLOB_METADATA_KEY).map(String::as_str) != Some("true") {
                return field.clone();
            }
            let mut metadata = field.metadata().clone();
            let native = blob_field(field.name(), field.is_nullable());
            metadata.extend(native.metadata().clone());
            Arc::new(native.with_metadata(metadata))
        })
        .collect::<Vec<_>>();
    Ok(Arc::new(Schema::new_with_metadata(
        fields,
        schema.metadata().clone(),
    )))
}

pub(crate) fn storage_batch(batch: RecordBatch, schema: &SchemaRef) -> Result<RecordBatch> {
    let columns = batch
        .columns()
        .iter()
        .zip(schema.fields())
        .map(|(array, field)| {
            if array.data_type() == field.data_type() {
                return Ok(array.clone());
            }
            // Only marked LargeBinary fields were changed by storage_schema.
            let data = array.as_any().downcast_ref::<LargeBinaryArray>().unwrap();
            let DataType::Struct(fields) = field.data_type() else {
                unreachable!()
            };
            let uri = Arc::new(StringArray::new_null(data.len())) as ArrayRef;
            let blob = StructArray::try_new(
                fields.clone(),
                vec![array.clone(), uri],
                data.nulls().cloned(),
            )
            .map_err(backend)?;
            Ok(Arc::new(blob) as ArrayRef)
        })
        .collect::<Result<Vec<_>>>()?;
    let options = arrow_array::RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));
    RecordBatch::try_new_with_options(schema.clone(), columns, &options).map_err(backend)
}
