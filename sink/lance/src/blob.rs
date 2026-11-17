//! Logical blob input conversion. Layout and object ownership belong to Lance.
use arrow_array::{ArrayRef, StructArray, make_array, new_null_array};
use arrow_cast::cast;
use arrow_schema::{DataType, Field};
use open_arrow_sink_core::{Error, RecordBatch, Result, SchemaRef};
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
        .zip(schema.fields())
        .map(|(array, field)| convert_blob_array(array.clone(), field.data_type()))
        .collect::<Result<Vec<_>>>()?;
    let options = arrow_array::RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));
    RecordBatch::try_new_with_options(schema.clone(), columns, &options).map_err(Error::backend)
}

fn convert_blob_array(array: ArrayRef, ty: &DataType) -> Result<ArrayRef> {
    if array.data_type() == ty {
        return Ok(array);
    }
    if array.data_type().is_string() || array.data_type() == &DataType::LargeBinary {
        let DataType::Struct(fields) = ty else {
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

        return Ok(Arc::new(StructArray::new(
            fields.clone(),
            vec![data, uris],
            array.nulls().cloned(),
        )));
    }
    let fields: &[Arc<Field>] = match ty {
        DataType::Struct(fields) => fields,
        DataType::List(item)
        | DataType::LargeList(item)
        | DataType::FixedSizeList(item, _)
        | DataType::Map(item, _) => std::slice::from_ref(item),
        _ => unreachable!("logical blob parent is a struct or list"),
    };
    let data = array.to_data();
    let children = fields
        .iter()
        .zip(data.child_data())
        .map(|(field, child)| {
            convert_blob_array(make_array(child.clone()), field.data_type())
                .map(|array| array.to_data())
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(make_array(
        data.into_builder()
            .data_type(ty.clone())
            .child_data(children)
            .build()
            .map_err(Error::backend)?,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Array, LargeStringArray, StringArray, StringViewArray};
    use arrow_schema::{Field, Schema};
    use open_arrow_sink_core::{BLOB_METADATA_KEY, WriteOptions, is_blob_field};

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
