use std::sync::Arc;

use arrow_array::RecordBatchOptions;
use arrow_schema::{DataType, Field, Fields, Schema};
use lance_encoding::encodings::physical::block::{CompressionConfig, CompressionScheme};
use open_arrow_sink_core::{
    Compression, CompressionOptions, Error, RecordBatch, Result, SchemaRef, StructuralEncoding,
};

const CODEC: &str = "lance-encoding:compression";
const STRUCTURAL: &str = "lance-encoding:structural-encoding";
const LEVEL: &str = "lance-encoding:compression-level";
const DICT_CODEC: &str = "lance-encoding:dict-values-compression";
const DICT_LEVEL: &str = "lance-encoding:dict-values-compression-level";

/// Convert shared codec requests to native Lance parameters before schema conversion.
fn native_compression(compression: Compression) -> Result<CompressionConfig> {
    let (scheme, level) = match compression {
        Compression::Uncompressed => (CompressionScheme::None, None),
        // Preserve Lance's native codec policy, including Fullzip's native fallback.
        Compression::Lz4Raw => (CompressionScheme::Lz4, None),
        Compression::Zstd { level } => {
            // Lance also accepts 0 as the native Zstd default.
            if let Some(level) = level
                && level != 0
                && !open_arrow_sink_core::ZSTD_COMPRESSION_LEVEL_RANGE.contains(&level)
            {
                return Err(crate::error::invalid_compression_level(level));
            }
            (CompressionScheme::Zstd, level)
        }
        _ => {
            return Err(Error::Unsupported(format!(
                "Lance compression: {compression:?}"
            )));
        }
    };
    Ok(CompressionConfig::new(scheme, level))
}

pub(crate) fn writer_schema(schema: &SchemaRef, options: &CompressionOptions) -> Result<SchemaRef> {
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
            return Err(invalid_path(path));
        }
        fields = nested_fields(&fields, path, native_compression(*codec)?)?;
    }
    Ok(Arc::new(Schema::new_with_metadata(
        fields,
        schema.metadata().clone(),
    )))
}

fn invalid_path(path: &[String]) -> Error {
    Error::Unsupported(format!(
        "nested compression path must identify one field: {path:?}"
    ))
}

fn nested_fields(fields: &Fields, path: &[String], codec: CompressionConfig) -> Result<Fields> {
    let (name, rest) = path.split_first().ok_or_else(|| invalid_path(path))?;
    let mut matches = fields.iter().enumerate().filter(|(_, f)| f.name() == name);
    let (index, field) = matches.next().ok_or_else(|| invalid_path(path))?;
    if matches.next().is_some() {
        return Err(invalid_path(path));
    }
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
        _ => return Err(invalid_path(path)),
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
        metadata.insert(CODEC.into(), codec.clone());
        // Dictionary values have an independent native compression default.
        metadata.insert(DICT_CODEC.into(), codec);
        metadata.remove(LEVEL);
        metadata.remove(DICT_LEVEL);
        if let Some(level) = compression.level() {
            metadata.insert(LEVEL.into(), level.to_string());
            metadata.insert(DICT_LEVEL.into(), level.to_string());
        }
    }
    if let Some(encoding) = encoding {
        metadata.insert(
            STRUCTURAL.into(),
            match encoding {
                StructuralEncoding::Miniblock => "miniblock",
                StructuralEncoding::Fullzip => "fullzip",
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

    #[test]
    fn invalid_native_compression_returns_an_error_without_prior_validation() {
        for compression in [
            Compression::Snappy,
            Compression::Zstd { level: Some(-1) },
            Compression::Zstd { level: Some(23) },
        ] {
            // Validate explicit global policies even when there are no fields.
            let schema = Arc::new(Schema::empty());
            assert!(matches!(
                writer_schema(
                    &schema,
                    &CompressionOptions {
                        global: Some(compression),
                        ..Default::default()
                    }
                ),
                Err(Error::Unsupported(_))
            ));
        }
    }

    #[test]
    fn invalid_nested_paths_return_errors_without_prior_validation() {
        let child = Arc::new(Field::new("value", DataType::Int64, false));
        let schema = Arc::new(Schema::new(vec![
            Field::new("scalar", DataType::Int64, false),
            Field::new(
                "nested",
                DataType::Struct(vec![child.clone()].into()),
                false,
            ),
            Field::new(
                "ambiguous",
                DataType::Struct(vec![child.clone(), child].into()),
                false,
            ),
            Field::new("duplicate", DataType::Int64, false),
            Field::new("duplicate", DataType::Int64, false),
        ]));
        for path in [
            vec![],
            vec!["nested"],
            vec!["missing", "value"],
            vec!["scalar", "value"],
            vec!["nested", "missing"],
            vec!["ambiguous", "value"],
            vec!["duplicate", "value"],
        ] {
            let options = CompressionOptions {
                nested_columns: [(
                    path.into_iter().map(String::from).collect(),
                    Compression::Uncompressed,
                )]
                .into(),
                ..Default::default()
            };
            assert!(matches!(
                writer_schema(&schema, &options),
                Err(Error::Unsupported(_))
            ));
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
