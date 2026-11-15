use open_arrow_sink_core::{CompressionCodec, CompressionOptions, Result, SchemaRef};
use parquet::{
    arrow::ArrowSchemaConverter, basic::Compression, file::properties::WriterProperties,
};

use crate::error::backend;

fn codec(codec: CompressionCodec) -> Compression {
    match codec {
        CompressionCodec::Uncompressed => Compression::UNCOMPRESSED,
        CompressionCodec::Snappy => Compression::SNAPPY,
        CompressionCodec::Gzip => Compression::GZIP(Default::default()),
        CompressionCodec::Zstd => Compression::ZSTD(Default::default()),
        CompressionCodec::Lz4Raw => Compression::LZ4_RAW,
    }
}

pub(crate) fn writer_properties(
    schema: &SchemaRef,
    options: &CompressionOptions,
) -> Result<WriterProperties> {
    let mut builder = WriterProperties::builder();
    if let Some(global) = options.global {
        builder = builder.set_compression(codec(global));
    }
    if !options.columns.is_empty() {
        let descriptor = ArrowSchemaConverter::new()
            .convert(schema)
            .map_err(backend)?;
        for (index, column) in descriptor.columns().iter().enumerate() {
            let root = descriptor.get_column_root_idx(index);
            if let Some(override_codec) = options.columns.get(schema.field(root).name()) {
                builder =
                    builder.set_column_compression(column.path().clone(), codec(*override_codec));
            }
        }
    }
    Ok(builder.build())
}
