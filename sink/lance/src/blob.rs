//! Logical blob input conversion. Layout and object ownership belong to Lance.
use arrow_array::{
    ArrayRef, LargeBinaryArray, LargeStringArray, StringArray, StringViewArray, StructArray,
};
use open_arrow_sink_core::{Error, RecordBatch, Result, SchemaRef, is_blob_field};
use std::sync::Arc;

const NATIVE_BLOB_EXTENSION: &str = "lance.blob.v2";

pub(crate) fn is_native_blob(field: &arrow_schema::Field) -> bool {
    field
        .metadata()
        .get("ARROW:extension:name")
        .map(String::as_str)
        == Some(NATIVE_BLOB_EXTENSION)
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
            let arrow_schema::DataType::Struct(fields) = target.data_type() else {
                unreachable!("logical blob field is a struct");
            };
            let (data, uris): (ArrayRef, ArrayRef) = match source.data_type() {
                arrow_schema::DataType::LargeBinary => {
                    (array.clone(), Arc::new(StringArray::new_null(array.len())))
                }
                arrow_schema::DataType::Utf8 => (
                    Arc::new(LargeBinaryArray::new_null(array.len())),
                    array.clone(),
                ),
                arrow_schema::DataType::LargeUtf8 => (
                    Arc::new(LargeBinaryArray::new_null(array.len())),
                    Arc::new(StringArray::from_iter(
                        array
                            .as_any()
                            .downcast_ref::<LargeStringArray>()
                            .unwrap()
                            .iter(),
                    )),
                ),
                arrow_schema::DataType::Utf8View => (
                    Arc::new(LargeBinaryArray::new_null(array.len())),
                    Arc::new(StringArray::from_iter(
                        array
                            .as_any()
                            .downcast_ref::<StringViewArray>()
                            .unwrap()
                            .iter(),
                    )),
                ),
                _ => unreachable!("validated blob input type"),
            };
            Arc::new(StructArray::new(
                fields.clone(),
                vec![data, uris],
                array.nulls().cloned(),
            )) as ArrayRef
        })
        .collect();
    let options = arrow_array::RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));
    RecordBatch::try_new_with_options(schema.clone(), columns, &options).map_err(Error::backend)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::Array;
    use arrow_schema::{Field, Schema};
    use open_arrow_sink_core::{BLOB_METADATA_KEY, WriteOptions};

    #[test]
    fn selected_text_inputs_become_logical_uris_without_changing_nulls() {
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(vec![Some("custom://blob"), None])),
            Arc::new(LargeStringArray::from(vec![Some("custom://blob"), None])),
            Arc::new(StringViewArray::from(vec![Some("custom://blob"), None])),
        ];
        let schema = Arc::new(Schema::new(
            arrays
                .iter()
                .enumerate()
                .map(|(index, array)| {
                    Field::new(format!("url{index}"), array.data_type().clone(), true)
                        .with_metadata([(BLOB_METADATA_KEY.into(), "true".into())].into())
                })
                .collect::<Vec<_>>(),
        ));
        let batch = RecordBatch::try_new(schema.clone(), arrays).unwrap();
        let schemas =
            crate::write_options::LanceSchemas::new(schema, &WriteOptions::default()).unwrap();
        for row in 0..2 {
            let logical = logical_batch(batch.slice(row, 1), &schemas.logical).unwrap();
            for column in logical.columns() {
                let blob = column.as_any().downcast_ref::<StructArray>().unwrap();
                assert_eq!(blob.is_null(0), row == 1);
                let uris = blob
                    .column_by_name("uri")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                assert_eq!(
                    uris.iter().next().unwrap(),
                    (row == 0).then_some("custom://blob")
                );
            }
        }
    }
}
