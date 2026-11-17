//! Logical blob input conversion. Layout and object ownership belong to Lance.
use arrow_array::{ArrayRef, StructArray, new_null_array};
use arrow_cast::cast;
use arrow_schema::{DataType, Field};
use open_arrow_sink_core::{Error, Result};
use std::sync::Arc;

pub(crate) fn is_native_blob(field: &arrow_schema::Field) -> bool {
    field
        .metadata()
        .get("ARROW:extension:name")
        .map(String::as_str)
        == Some("lance.blob.v2")
}

/// Convert a selected binary or URL leaf; the materializer owns container traversal.
pub(crate) fn convert_blob_leaf(array: &ArrayRef, field: &Field) -> Result<ArrayRef> {
    if matches!(array.data_type(), DataType::Struct(_)) {
        return cast(array, field.data_type()).map_err(Error::backend);
    }
    let DataType::Struct(fields) = field.data_type() else {
        unreachable!("logical blob field is a struct")
    };
    let (data, uris) = if array.data_type() == &DataType::LargeBinary {
        (array.clone(), new_null_array(&DataType::Utf8, array.len()))
    } else {
        (
            new_null_array(&DataType::LargeBinary, array.len()),
            cast(&array, &DataType::Utf8).map_err(Error::backend)?,
        )
    };

    Ok(Arc::new(StructArray::new(
        fields.clone(),
        vec![data, uris],
        array.nulls().cloned(),
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Array, LargeStringArray, RecordBatch, StringArray, StringViewArray};
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
            let batch = batch.slice(row, 1);
            for (column, field) in batch.columns().iter().zip(schemas.logical.fields()) {
                let logical = convert_blob_leaf(column, field).unwrap();
                let blob = logical.as_any().downcast_ref::<StructArray>().unwrap();
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
