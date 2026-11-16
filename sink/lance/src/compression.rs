use std::sync::Arc;

use arrow_array::RecordBatchOptions;
use arrow_schema::{DataType, Field, Schema};
use lance_encoding::constants::{
    COMPRESSION_LEVEL_META_KEY, COMPRESSION_META_KEY, DICT_VALUES_COMPRESSION_LEVEL_META_KEY,
    DICT_VALUES_COMPRESSION_META_KEY, STRUCTURAL_ENCODING_FULLZIP, STRUCTURAL_ENCODING_META_KEY,
    STRUCTURAL_ENCODING_MINIBLOCK,
};
use open_arrow_sink_core::{
    Compression, CompressionOptions, Error, RecordBatch, Result, SchemaRef, StructuralEncoding,
};

pub(crate) fn apply_compression(schema: &SchemaRef, options: &CompressionOptions) -> SchemaRef {
    if options.global.is_none()
        && options.columns.is_empty()
        && options.nested_columns.is_empty()
        && options.structural_encoding.is_none()
        && options.column_encodings.is_empty()
    {
        return schema.clone();
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
                field.clone()
            } else {
                Arc::new(compression_field(field, codec, encoding))
            }
        })
        .collect();
    for (path, codec) in &options.nested_columns {
        let index = fields
            .iter()
            .position(|f| f.name() == &path[0])
            .expect("validated path");
        fields[index] = nested_override(&fields[index], &path[1..], *codec);
    }
    Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone()))
}

fn nested_override(field: &Arc<Field>, path: &[String], codec: Compression) -> Arc<Field> {
    if path.is_empty() {
        return Arc::new(compression_field(field, Some(codec), None));
    }
    let ty = match field.data_type() {
        DataType::Struct(children) => DataType::Struct(
            children
                .iter()
                .map(|child| {
                    if child.name() == &path[0] {
                        nested_override(child, &path[1..], codec)
                    } else {
                        child.clone()
                    }
                })
                .collect(),
        ),
        DataType::List(item) => DataType::List(nested_override(item, path, codec)),
        DataType::LargeList(item) => DataType::LargeList(nested_override(item, path, codec)),
        DataType::FixedSizeList(item, size) => {
            DataType::FixedSizeList(nested_override(item, path, codec), *size)
        }
        _ => unreachable!("nested path validated before schema conversion"),
    };
    Arc::new(field.as_ref().clone().with_data_type(ty))
}

fn compression_field(
    field: &Field,
    compression: Option<Compression>,
    encoding: Option<StructuralEncoding>,
) -> Field {
    let mut metadata = field.metadata().clone();
    if let Some(compression) = compression {
        // Capability validation rejects all other codecs before building this schema.
        let codec = match compression {
            Compression::Uncompressed => "none",
            Compression::Zstd { .. } => "zstd",
            _ => unreachable!("compression must be validated before schema conversion"),
        };
        metadata.insert(COMPRESSION_META_KEY.into(), codec.into());
        // Dictionary values have an independent native compression default.
        metadata.insert(DICT_VALUES_COMPRESSION_META_KEY.into(), codec.into());
        metadata.remove(COMPRESSION_LEVEL_META_KEY);
        metadata.remove(DICT_VALUES_COMPRESSION_LEVEL_META_KEY);
        if let Compression::Zstd { level: Some(level) } = compression {
            metadata.insert(COMPRESSION_LEVEL_META_KEY.into(), level.to_string());
            metadata.insert(
                DICT_VALUES_COMPRESSION_LEVEL_META_KEY.into(),
                level.to_string(),
            );
        }
    }
    if let Some(encoding) = encoding {
        metadata.insert(
            STRUCTURAL_ENCODING_META_KEY.into(),
            match encoding {
                StructuralEncoding::Miniblock => STRUCTURAL_ENCODING_MINIBLOCK,
                StructuralEncoding::Fullzip => STRUCTURAL_ENCODING_FULLZIP,
            }
            .into(),
        );
    }
    field
        .clone()
        .with_data_type(compression_type(field.data_type(), compression, encoding))
        .with_metadata(metadata)
}

fn compression_type(
    data_type: &DataType,
    compression: Option<Compression>,
    encoding: Option<StructuralEncoding>,
) -> DataType {
    let child = |field: &Arc<Field>| Arc::new(compression_field(field, compression, encoding));
    match data_type {
        DataType::Struct(fields) => DataType::Struct(fields.iter().map(child).collect()),
        DataType::List(field) => DataType::List(child(field)),
        DataType::LargeList(field) => DataType::LargeList(child(field)),
        DataType::ListView(field) => DataType::ListView(child(field)),
        DataType::LargeListView(field) => DataType::LargeListView(child(field)),
        DataType::FixedSizeList(field, size) => DataType::FixedSizeList(child(field), *size),
        DataType::Map(field, sorted) => DataType::Map(child(field), *sorted),
        DataType::Dictionary(key, value) => DataType::Dictionary(
            key.clone(),
            Box::new(compression_type(value, compression, encoding)),
        ),
        DataType::Union(fields, mode) => DataType::Union(
            fields
                .iter()
                .map(|(id, field)| (id, child(field)))
                .collect(),
            *mode,
        ),
        DataType::RunEndEncoded(ends, values) => {
            DataType::RunEndEncoded(child(ends), child(values))
        }
        _ => data_type.clone(),
    }
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
    fn omitted_policies_preserve_defaults_and_unrelated_metadata() {
        let child = Field::new("child", DataType::Int64, false).with_metadata(
            [
                (COMPRESSION_META_KEY.into(), "zstd".into()),
                (COMPRESSION_LEVEL_META_KEY.into(), "7".into()),
                (STRUCTURAL_ENCODING_META_KEY.into(), "miniblock".into()),
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
            &apply_compression(&schema, &Default::default())
        ));
        let encoded = apply_compression(
            &schema,
            &CompressionOptions {
                structural_encoding: Some(StructuralEncoding::Fullzip),
                ..Default::default()
            },
        );
        let DataType::Struct(fields) = encoded.field(0).data_type() else {
            panic!("expected struct")
        };
        let metadata = fields[0].metadata();
        assert_eq!(metadata[COMPRESSION_META_KEY], "zstd");
        assert_eq!(metadata[COMPRESSION_LEVEL_META_KEY], "7");
        assert_eq!(metadata[STRUCTURAL_ENCODING_META_KEY], "fullzip");
        assert_eq!(metadata["custom"], "preserved");
        let codec_only = apply_compression(
            &encoded,
            &CompressionOptions {
                global: Some(Compression::Uncompressed),
                ..Default::default()
            },
        );
        let DataType::Struct(fields) = codec_only.field(0).data_type() else {
            panic!("expected struct")
        };
        assert_eq!(
            fields[0].metadata()[STRUCTURAL_ENCODING_META_KEY],
            "fullzip"
        );
        assert_eq!(fields[0].metadata()[COMPRESSION_META_KEY], "none");
        assert!(
            !fields[0]
                .metadata()
                .contains_key(COMPRESSION_LEVEL_META_KEY)
        );
    }
}
