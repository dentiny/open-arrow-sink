use open_arrow_sink_core::{
    Compression as SinkCompression, CompressionCodec, CompressionOptions, Result, SchemaRef,
};
use parquet::{
    arrow::ArrowSchemaConverter,
    basic::{Compression, ZstdLevel},
    file::properties::WriterProperties,
};

use crate::error::backend;

pub(crate) const SUPPORTED_CODECS: &[CompressionCodec] = &[
    CompressionCodec::Uncompressed,
    #[cfg(feature = "snappy")]
    CompressionCodec::Snappy,
    #[cfg(feature = "gzip")]
    CompressionCodec::Gzip,
    #[cfg(feature = "zstd")]
    CompressionCodec::Zstd,
    #[cfg(feature = "lz4")]
    CompressionCodec::Lz4Raw,
];

fn codec(codec: SinkCompression) -> Result<Compression> {
    Ok(match codec {
        SinkCompression::Uncompressed => Compression::UNCOMPRESSED,
        SinkCompression::Snappy => Compression::SNAPPY,
        SinkCompression::Gzip => Compression::GZIP(Default::default()),
        SinkCompression::Zstd { level } => Compression::ZSTD(match level {
            Some(level) => ZstdLevel::try_new(level).map_err(backend)?,
            None => ZstdLevel::default(),
        }),
        SinkCompression::Lz4Raw => Compression::LZ4_RAW,
    })
}

pub(crate) fn writer_properties(
    schema: &SchemaRef,
    options: &CompressionOptions,
) -> Result<WriterProperties> {
    let mut builder = WriterProperties::builder();
    if let Some(global) = options.global {
        builder = builder.set_compression(codec(global)?);
    }
    if !options.columns.is_empty() {
        let descriptor = ArrowSchemaConverter::new()
            .convert(schema)
            .map_err(backend)?;
        for (index, column) in descriptor.columns().iter().enumerate() {
            let root = descriptor.get_column_root_idx(index);
            if let Some(override_codec) = options.columns.get(schema.field(root).name()) {
                builder =
                    builder.set_column_compression(column.path().clone(), codec(*override_codec)?);
            }
        }
    }
    Ok(builder.build())
}
