use std::sync::Arc;

use arrow_array::RecordBatchOptions;
use arrow_schema::{DataType, Field, Schema};
use open_arrow_sink_core::{Compression, CompressionOptions, RecordBatch, Result, SchemaRef};

use crate::error::backend;

const CODEC: &str = "lance-encoding:compression";
const LEVEL: &str = "lance-encoding:compression-level";
const DICT_CODEC: &str = "lance-encoding:dict-values-compression";
const DICT_LEVEL: &str = "lance-encoding:dict-values-compression-level";

pub(crate) fn writer_schema(schema: &SchemaRef, options: &CompressionOptions) -> SchemaRef {
    if options.global.is_none() && options.columns.is_empty() {
        return schema.clone();
    }
    let fields: Vec<_> = schema
        .fields()
        .iter()
        .map(|field| {
            match options
                .columns
                .get(field.name())
                .copied()
                .or(options.global)
            {
                Some(compression) => Arc::new(compression_field(field, compression)),
                None => field.clone(),
            }
        })
        .collect();
    Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone()))
}

fn compression_field(field: &Field, compression: Compression) -> Field {
    let mut metadata = field.metadata().clone();
    // Capability validation rejects all other codecs before building this schema.
    let codec = match compression {
        Compression::Uncompressed => "none",
        Compression::Zstd { .. } => "zstd",
        _ => unreachable!("compression must be validated before schema conversion"),
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
    field
        .clone()
        .with_data_type(compression_type(field.data_type(), compression))
        .with_metadata(metadata)
}

fn compression_type(data_type: &DataType, compression: Compression) -> DataType {
    let child = |field: &Arc<Field>| Arc::new(compression_field(field, compression));
    match data_type {
        DataType::Struct(fields) => DataType::Struct(fields.iter().map(child).collect()),
        DataType::List(field) => DataType::List(child(field)),
        DataType::LargeList(field) => DataType::LargeList(child(field)),
        DataType::ListView(field) => DataType::ListView(child(field)),
        DataType::LargeListView(field) => DataType::LargeListView(child(field)),
        DataType::FixedSizeList(field, size) => DataType::FixedSizeList(child(field), *size),
        DataType::Map(field, sorted) => DataType::Map(child(field), *sorted),
        DataType::Dictionary(key, value) => {
            DataType::Dictionary(key.clone(), Box::new(compression_type(value, compression)))
        }
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
        .map_err(backend)
}
