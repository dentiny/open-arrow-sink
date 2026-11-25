//! Logical blob input conversion. Layout and object ownership belong to Lance.
use arrow_array::{Array, ArrayRef, StructArray, new_null_array};
use arrow_cast::cast;
use arrow_schema::{DataType, Field, Fields};
use open_arrow_sink_core::{Error, Result};
use std::sync::Arc;

const NATIVE_BLOB_EXTENSION: &str = "lance.blob.v2";

pub(crate) fn is_native_blob(field: &arrow_schema::Field) -> bool {
    field
        .metadata()
        .get("ARROW:extension:name")
        .map(String::as_str)
        == Some(NATIVE_BLOB_EXTENSION)
}

/// Wrap selected bytes/URLs in Lance's logical `{data, uri}` representation.
/// This performs no I/O. The materializer calls it after slicing list children,
/// then uses ancestor visibility to decide which values to download or buffer.
/// Native logical Blob inputs already have the struct shape; only metadata may differ.
pub(crate) fn convert_blob_leaf(array: &ArrayRef, field: &Field) -> Result<ArrayRef> {
    if matches!(array.data_type(), DataType::Struct(_)) {
        return cast(array, field.data_type()).map_err(Error::backend);
    }
    let DataType::Struct(fields) = field.data_type() else {
        unreachable!("logical blob field is a struct")
    };
    let (data, uris) = if matches!(
        array.data_type(),
        DataType::Binary | DataType::LargeBinary | DataType::BinaryView
    ) {
        (
            cast(array, &DataType::LargeBinary).map_err(Error::backend)?,
            new_null_array(&DataType::Utf8, array.len()),
        )
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

/// Replace the payload columns while preserving nulls and optional reference fields.
/// Once bytes have been materialized, position/size no longer describe a URI
/// reference and must be cleared for those values.
pub(crate) fn with_materialized_payloads(
    values: &StructArray,
    fields: &Fields,
    data: ArrayRef,
    uris: ArrayRef,
    materialized: &[bool],
) -> Result<ArrayRef> {
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
    Ok(Arc::new(
        StructArray::try_new(fields.clone(), columns, values.nulls().cloned())
            .map_err(Error::backend)?,
    ) as ArrayRef)
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
