use std::sync::Arc;

use crate::error;

use arrow_array::RecordBatchOptions;
use arrow_schema::{DataType, Field, Fields, Schema};
use lance_encoding::constants::{
    COMPRESSION_LEVEL_META_KEY, COMPRESSION_META_KEY, DICT_VALUES_COMPRESSION_LEVEL_META_KEY,
    DICT_VALUES_COMPRESSION_META_KEY, STRUCTURAL_ENCODING_FULLZIP, STRUCTURAL_ENCODING_META_KEY,
    STRUCTURAL_ENCODING_MINIBLOCK,
};
use lance_encoding::encodings::physical::block::{CompressionConfig, CompressionScheme};
use open_arrow_sink_core::{
    CompressionOptions, CompressionSpec, Error, RecordBatch, Result, SchemaRef, StructuralEncoding,
    is_blob_field, validation,
};

pub(crate) fn validate_encoding(schema: &Schema, options: &CompressionOptions) -> Result<()> {
    // Native blob descriptors are opaque to Lance users. Keep this backend rule
    // here rather than teaching core's Arrow path resolver about Lance metadata.
    for path in options.nested_columns.keys() {
        if (1..path.len()).any(|end| {
            validation::nested_field(schema, &path[..end]).is_some_and(|field| {
                field
                    .metadata()
                    .get("ARROW:extension:name")
                    .map(String::as_str)
                    == Some("lance.blob.v2")
            })
        }) {
            return Err(error::nested_blob_compression(path));
        }
    }
    for field in schema.fields() {
        let encoding = options
            .column_encodings
            .get(field.name())
            .copied()
            .or(options.structural_encoding);
        if encoding.is_some()
            && (is_blob_field(field)
                || field
                    .metadata()
                    .get("ARROW:extension:name")
                    .map(String::as_str)
                    == Some("lance.blob.v2"))
        {
            return Err(error::blob_structural_encoding(field.name()));
        }
    }
    Ok(())
}

/// Convert shared codec requests to native Lance parameters before schema conversion.
pub(crate) fn native_compression(compression: CompressionSpec) -> Result<CompressionConfig> {
    let (scheme, level) = match compression {
        CompressionSpec::Uncompressed => (CompressionScheme::None, None),
        // Preserve Lance's native codec policy, including Fullzip's native fallback.
        CompressionSpec::Lz4Raw => (CompressionScheme::Lz4, None),
        CompressionSpec::Zstd { level } => {
            // Lance also accepts 0 as the native Zstd default.
            if let Some(level) = level
                && level != 0
                && !open_arrow_sink_core::ZSTD_COMPRESSION_LEVEL_RANGE.contains(&level)
            {
                return Err(error::invalid_compression_level(level));
            }
            (CompressionScheme::Zstd, level)
        }
        _ => {
            return Err(error::unsupported_compression(compression));
        }
    };
    Ok(CompressionConfig::new(scheme, level))
}

pub(crate) fn apply_compression(
    schema: &SchemaRef,
    options: &CompressionOptions,
) -> Result<SchemaRef> {
    if options.global.is_none()
        && options.columns.is_empty()
        && options.nested_columns.is_empty()
        && options.structural_encoding.is_none()
        && options.column_encodings.is_empty()
    {
        return Ok(schema.clone());
    }
    let global = options.global.map(native_compression).transpose()?;
    let mut fields = schema
        .fields()
        .iter()
        .map(|field| {
            let codec = options
                .columns
                .get(field.name())
                .copied()
                .map(native_compression)
                .transpose()?
                .or(global);
            let encoding = options
                .column_encodings
                .get(field.name())
                .copied()
                .or(options.structural_encoding);
            Ok(if codec.is_none() && encoding.is_none() {
                field.clone()
            } else {
                Arc::new(compression_field(field, codec, encoding))
            })
        })
        .collect::<Result<Fields>>()?;
    for (path, codec) in &options.nested_columns {
        if path.len() < 2 {
            return Err(error::invalid_compression_path(path));
        }
        fields = nested_fields(&fields, path, native_compression(*codec)?)?;
    }
    Ok(Arc::new(Schema::new_with_metadata(
        fields,
        schema.metadata().clone(),
    )))
}

fn nested_fields(fields: &Fields, path: &[String], codec: CompressionConfig) -> Result<Fields> {
    let (name, rest) = path
        .split_first()
        .ok_or_else(|| error::invalid_compression_path(path))?;
    let index = validation::unique_field_index(fields, name)
        .ok_or_else(|| error::invalid_compression_path(path))?;
    let field = &fields[index];
    let replacement = nested_override(field, rest, codec)?;
    let mut fields = fields.to_vec();
    fields[index] = replacement;
    Ok(fields.into())
}

fn nested_override(
    field: &Arc<Field>,
    path: &[String],
    codec: CompressionConfig,
) -> Result<Arc<Field>> {
    if path.is_empty() {
        return Ok(Arc::new(compression_field(field, Some(codec), None)));
    }
    let ty = match field.data_type() {
        DataType::Struct(children) => DataType::Struct(nested_fields(children, path, codec)?),
        DataType::List(item) => DataType::List(nested_override(item, path, codec)?),
        DataType::LargeList(item) => DataType::LargeList(nested_override(item, path, codec)?),
        DataType::FixedSizeList(item, size) => {
            DataType::FixedSizeList(nested_override(item, path, codec)?, *size)
        }
        _ => return Err(error::invalid_compression_path(path)),
    };
    Ok(Arc::new(field.as_ref().clone().with_data_type(ty)))
}

fn compression_field(
    field: &Field,
    compression: Option<CompressionConfig>,
    encoding: Option<StructuralEncoding>,
) -> Field {
    let mut metadata = field.metadata().clone();
    if let Some(compression) = compression {
        let codec = compression.scheme().to_string();
        metadata.insert(COMPRESSION_META_KEY.into(), codec.clone());
        // Dictionary values have an independent native compression default.
        metadata.insert(DICT_VALUES_COMPRESSION_META_KEY.into(), codec);
        metadata.remove(COMPRESSION_LEVEL_META_KEY);
        metadata.remove(DICT_VALUES_COMPRESSION_LEVEL_META_KEY);
        if let Some(level) = compression.level() {
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
    compression: Option<CompressionConfig>,
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
    use open_arrow_sink_core::CompressionSpec;

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
            &apply_compression(&schema, &Default::default()).unwrap()
        ));
        let encoded = apply_compression(
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
        assert_eq!(metadata[COMPRESSION_META_KEY], "zstd");
        assert_eq!(metadata[COMPRESSION_LEVEL_META_KEY], "7");
        assert_eq!(metadata[STRUCTURAL_ENCODING_META_KEY], "fullzip");
        assert_eq!(metadata["custom"], "preserved");
        let codec_only = apply_compression(
            &encoded,
            &CompressionOptions {
                global: Some(CompressionSpec::Uncompressed),
                ..Default::default()
            },
        )
        .unwrap();
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
