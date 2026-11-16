use std::sync::Arc;

use arrow_array::RecordBatchOptions;
use arrow_schema::{DataType, Field, Schema};
use open_arrow_sink_core::{
    Compression, CompressionOptions, Error, RecordBatch, Result, SchemaRef, StructuralEncoding,
};


use lance_encoding::constants::{
    COMPRESSION_LEVEL_META_KEY as LEVEL, COMPRESSION_META_KEY as CODEC,
    DICT_VALUES_COMPRESSION_LEVEL_META_KEY as DICT_LEVEL,
    DICT_VALUES_COMPRESSION_META_KEY as DICT_CODEC, STRUCTURAL_ENCODING_FULLZIP,
    STRUCTURAL_ENCODING_META_KEY as STRUCTURAL, STRUCTURAL_ENCODING_MINIBLOCK,
};

pub(crate) fn writer_schema(schema: &SchemaRef, options: &CompressionOptions) -> Result<SchemaRef> {
    if options.global.is_none()
        && options.columns.is_empty()
        && options.nested_columns.is_empty()
        && options.structural_encoding.is_none()
        && options.column_encodings.is_empty()
    {
        return Ok(schema.clone());
    }
    let mut fields: Vec<_> = schema
        .fields()
        .iter()
        .map(|field| {
            let codec = options
                .columns
                .get(field.name())
                .copied()
                .or(options.global);
            let encoding = options
                .column_encodings
                .get(field.name())
                .copied()
                .or(options.structural_encoding);
            if codec.is_none() && encoding.is_none() {
                Ok(field.clone())
            } else {
                compression_field(field, codec, encoding).map(Arc::new)
            }
        })
        .collect::<Result<_>>()?;
    for (path, codec) in &options.nested_columns {
        let (head, tail) = path
            .split_first()
            .ok_or_else(|| Error::Unsupported("nested compression path cannot be empty".into()))?;
        let index = fields
            .iter()
            .position(|f| f.name() == head)
            .ok_or_else(|| Error::Unsupported(format!("unknown compression column: {head}")))?;
        fields[index] = nested_override(&fields[index], tail, *codec)?;
    }
    Ok(Arc::new(Schema::new_with_metadata(
        fields,
        schema.metadata().clone(),
    )))
}

fn nested_override(field: &Arc<Field>, path: &[String], codec: Compression) -> Result<Arc<Field>> {
    if path.is_empty() {
        return compression_field(field, Some(codec), None).map(Arc::new);
    }
    let ty = match field.data_type() {
        DataType::Struct(children) => {
            if !children.iter().any(|child| child.name() == &path[0]) {
                return Err(Error::Unsupported(format!(
                    "unknown nested compression column: {path:?}"
                )));
            }
            DataType::Struct(
                children
                    .iter()
                    .map(|child| {
                        if child.name() == &path[0] {
                            nested_override(child, &path[1..], codec)
                        } else {
                            Ok(child.clone())
                        }
                    })
                    .collect::<Result<Vec<_>>>()?
                    .into(),
            )
        }
        DataType::List(item) => DataType::List(nested_override(item, path, codec)?),
        DataType::LargeList(item) => DataType::LargeList(nested_override(item, path, codec)?),
        DataType::FixedSizeList(item, size) => {
            DataType::FixedSizeList(nested_override(item, path, codec)?, *size)
        }
        _ => {
            return Err(Error::Unsupported(format!(
                "invalid nested compression path: {path:?}"
            )));
        }
    };
    Ok(Arc::new(field.as_ref().clone().with_data_type(ty)))
}

fn compression_field(
    field: &Field,
    compression: Option<Compression>,
    encoding: Option<StructuralEncoding>,
) -> Result<Field> {
    let mut metadata = field.metadata().clone();
    if let Some(compression) = compression {
        let codec = match compression {
            Compression::Uncompressed => "none",
            Compression::Zstd { .. } => "zstd",
            other => return Err(Error::Unsupported(format!("Lance compression: {other:?}"))),
        };
        metadata.insert(CODEC.into(), codec.into());
        // Dictionary values have an independent native compression default.
        metadata.insert(DICT_CODEC.into(), codec.into());
        metadata.remove(LEVEL);
        metadata.remove(DICT_LEVEL);
        if let Compression::Zstd { level: Some(level) } = compression {
            metadata.insert(LEVEL.into(), level.to_string());
            metadata.insert(DICT_LEVEL.into(), level.to_string());
        }
    }
    if let Some(encoding) = encoding {
        metadata.insert(
            STRUCTURAL.into(),
            match encoding {
                StructuralEncoding::Miniblock => STRUCTURAL_ENCODING_MINIBLOCK,
                StructuralEncoding::Fullzip => STRUCTURAL_ENCODING_FULLZIP,
            }
            .into(),
        );
    }
    Ok(field
        .clone()
        .with_data_type(compression_type(field.data_type(), compression, encoding)?)
        .with_metadata(metadata))
}

fn compression_type(
    data_type: &DataType,
    compression: Option<Compression>,
    encoding: Option<StructuralEncoding>,
) -> Result<DataType> {
    let child = |field: &Arc<Field>| compression_field(field, compression, encoding).map(Arc::new);
    Ok(match data_type {
        DataType::Struct(fields) => {
            DataType::Struct(fields.iter().map(child).collect::<Result<Vec<_>>>()?.into())
        }
        DataType::List(field) => DataType::List(child(field)?),
        DataType::LargeList(field) => DataType::LargeList(child(field)?),
        DataType::ListView(field) => DataType::ListView(child(field)?),
        DataType::LargeListView(field) => DataType::LargeListView(child(field)?),
        DataType::FixedSizeList(field, size) => DataType::FixedSizeList(child(field)?, *size),
        DataType::Map(field, sorted) => DataType::Map(child(field)?, *sorted),
        DataType::Dictionary(key, value) => DataType::Dictionary(
            key.clone(),
            Box::new(compression_type(value, compression, encoding)?),
        ),
        DataType::Union(fields, mode) => DataType::Union(
            fields
                .iter()
                .map(|(id, field)| child(field).map(|field| (id, field)))
                .collect::<Result<arrow_schema::UnionFields>>()?,
            *mode,
        ),
        DataType::RunEndEncoded(ends, values) => {
            DataType::RunEndEncoded(child(ends)?, child(values)?)
        }
        _ => data_type.clone(),
    })
}

pub(crate) fn writer_batch(batch: RecordBatch, schema: &SchemaRef) -> Result<RecordBatch> {
    // This is called only after exact validation against the original schema.
    // Only metadata differs, including nested field metadata. Reuse all buffers.
    let options = RecordBatchOptions::new()
        .with_match_field_names(false)
        .with_row_count(Some(batch.num_rows()));
    RecordBatch::try_new_with_options(schema.clone(), batch.columns().to_vec(), &options)
        .map_err(Error::backend)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::Field;

    #[test]
    fn unsupported_codec_and_malformed_paths_return_errors() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        assert!(
            writer_schema(
                &schema,
                &CompressionOptions {
                    global: Some(Compression::Snappy),
                    ..Default::default()
                }
            )
            .is_err()
        );
        for path in [
            vec![],
            vec!["missing".into()],
            vec!["id".into(), "child".into()],
        ] {
            assert!(
                writer_schema(
                    &schema,
                    &CompressionOptions {
                        nested_columns: [(path, Compression::Uncompressed)].into(),
                        ..Default::default()
                    }
                )
                .is_err()
            );
        }
    }

    #[test]
    fn omitted_policies_preserve_defaults_and_unrelated_metadata() {
        let child = Field::new("child", DataType::Int64, false).with_metadata(
            [
                (CODEC.into(), "zstd".into()),
                (LEVEL.into(), "7".into()),
                (STRUCTURAL.into(), "miniblock".into()),
                ("custom".into(), "preserved".into()),
            ]
            .into(),
        );
        let schema = Arc::new(Schema::new(vec![Field::new(
            "nested",
            DataType::Struct(vec![Arc::new(child)].into()),
            false,
        )]));
        assert!(Arc::ptr_eq(
            &schema,
            &writer_schema(&schema, &Default::default()).unwrap()
        ));
        let encoded = writer_schema(
            &schema,
            &CompressionOptions {
                structural_encoding: Some(StructuralEncoding::Fullzip),
                ..Default::default()
            },
        )
        .unwrap();
        let DataType::Struct(fields) = encoded.field(0).data_type() else {
            panic!("expected struct")
        };
        let metadata = fields[0].metadata();
        assert_eq!(metadata[CODEC], "zstd");
        assert_eq!(metadata[LEVEL], "7");
        assert_eq!(metadata[STRUCTURAL], "fullzip");
        assert_eq!(metadata["custom"], "preserved");
        let codec_only = writer_schema(
            &encoded,
            &CompressionOptions {
                global: Some(Compression::Uncompressed),
                ..Default::default()
            },
        )
        .unwrap();
        let DataType::Struct(fields) = codec_only.field(0).data_type() else {
            panic!("expected struct")
        };
        assert_eq!(fields[0].metadata()[STRUCTURAL], "fullzip");
        assert_eq!(fields[0].metadata()[CODEC], "none");
        assert!(!fields[0].metadata().contains_key(LEVEL));
    }
}
