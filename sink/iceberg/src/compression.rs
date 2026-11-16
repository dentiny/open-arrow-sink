use std::collections::HashMap;

use iceberg::spec::TableProperties;
use open_arrow_sink_core::CompressionSpec;

pub(crate) fn apply_compression(
    properties: &mut HashMap<String, String>,
    compression: Option<CompressionSpec>,
) {
    if let Some(codec) = compression {
        let value = match codec {
            CompressionSpec::Uncompressed => "uncompressed",
            CompressionSpec::Snappy => "snappy",
            CompressionSpec::Gzip => "gzip",
            CompressionSpec::Zstd { .. } => "zstd",
            CompressionSpec::Lz4Raw => "lz4_raw",
        };
        properties.insert(
            TableProperties::PROPERTY_PARQUET_COMPRESSION_CODEC.into(),
            value.into(),
        );
        // An explicit codec replaces the table's old codec and level.
        properties.remove(TableProperties::PROPERTY_PARQUET_COMPRESSION_LEVEL);
        if let CompressionSpec::Zstd { level: Some(level) } = codec {
            properties.insert(
                TableProperties::PROPERTY_PARQUET_COMPRESSION_LEVEL.into(),
                level.to_string(),
            );
        }
    }
}
