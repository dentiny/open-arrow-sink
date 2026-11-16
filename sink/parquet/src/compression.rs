use open_arrow_sink_core::{
    CompressionCodec, CompressionOptions, CompressionSpec, Error, Result, SchemaRef,
};
use parquet::{
    arrow::ArrowSchemaConverter,
    basic::{Compression, ZstdLevel},
    file::properties::WriterProperties,
};

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

fn codec(codec: CompressionSpec) -> Result<Compression> {
    Ok(match codec {
        CompressionSpec::Uncompressed => Compression::UNCOMPRESSED,
        CompressionSpec::Snappy => Compression::SNAPPY,
        CompressionSpec::Gzip => Compression::GZIP(Default::default()),
        CompressionSpec::Zstd { level } => Compression::ZSTD(match level {
            Some(level) => ZstdLevel::try_new(level)
                .map_err(|error| Error::InvalidConfig(error.to_string()))?,
            None => ZstdLevel::default(),
        }),
        CompressionSpec::Lz4Raw => Compression::LZ4_RAW,
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
            .map_err(Error::backend)?;
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
