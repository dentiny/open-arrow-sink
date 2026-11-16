use std::collections::HashMap;

use iceberg::spec::TableProperties;
use open_arrow_sink_core::Compression;

pub(crate) fn apply_compression(
    properties: &mut HashMap<String, String>,
    compression: Option<Compression>,
) {
    if let Some(codec) = compression {
        let value = match codec {
            Compression::Uncompressed => "uncompressed",
            Compression::Snappy => "snappy",
            Compression::Gzip => "gzip",
            Compression::Zstd { .. } => "zstd",
            Compression::Lz4Raw => "lz4_raw",
        };
        properties.insert(
            TableProperties::PROPERTY_PARQUET_COMPRESSION_CODEC.into(),
            value.into(),
        );
        // An explicit codec replaces the table's old codec and level.
        properties.remove(TableProperties::PROPERTY_PARQUET_COMPRESSION_LEVEL);
        if let Compression::Zstd { level: Some(level) } = codec {
            properties.insert(
                TableProperties::PROPERTY_PARQUET_COMPRESSION_LEVEL.into(),
                level.to_string(),
            );
        }
    }
}
