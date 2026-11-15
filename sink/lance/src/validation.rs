//! Configuration validation before destination creation.
use arrow_schema::{DataType, Schema};
use open_arrow_sink_core::{
    Compression, IndexSpec, IndexType, Result, SinkCapability, WriteOptions,
    ZSTD_COMPRESSION_LEVEL_RANGE, validation,
};

use crate::error;

pub(crate) fn validate_options(
    schema: &Schema,
    options: &WriteOptions,
    capability: SinkCapability,
) -> Result<()> {
    validation::validate_options(schema, options, capability)?;
    for compression in options
        .compression
        .global
        .iter()
        .chain(options.compression.columns.values())
    {
        // Lance accepts 0 as the native Zstd default, in addition to positive levels.
        if let Compression::Zstd { level: Some(level) } = compression
            && *level != 0
            && !ZSTD_COMPRESSION_LEVEL_RANGE.contains(level)
        {
            return Err(error::invalid_compression_level(*level));
        }
    }
    validate_indexes(schema, &options.indexes)
}

pub(crate) fn validate_indexes(schema: &Schema, indexes: &[IndexSpec]) -> Result<()> {
    use DataType::*;
    validation::validate_indexes(schema, indexes)?;
    for index in indexes {
        let field = schema.field_with_name(&index.column).map_err(|_| {
            open_arrow_sink_core::Error::Unsupported(format!(
                "index column not found: {}",
                index.column
            ))
        })?;
        let supported = match index.index_type {
            IndexType::Scalar => matches!(
                field.data_type(),
                Boolean
                    | Int8
                    | Int16
                    | Int32
                    | Int64
                    | UInt8
                    | UInt16
                    | UInt32
                    | UInt64
                    | Float16
                    | Float32
                    | Float64
                    | Decimal128(_, _)
                    | Utf8
                    | LargeUtf8
                    | Utf8View
                    | Date32
                    | Date64
                    | Time32(_)
                    | Time64(_)
                    | Timestamp(_, _)
            ),
            // Lance 14's IVF_FLAT L2 path supports floating-point vectors.
            // Int8 is rejected even though the older reference allowed it.
            IndexType::Vector => {
                matches!(field.data_type(), FixedSizeList(child, dimension) if *dimension > 0 && matches!(child.data_type(), Float16 | Float32 | Float64))
            }
            IndexType::Text => matches!(field.data_type(), Utf8 | LargeUtf8 | Utf8View),
            IndexType::BloomFilter => matches!(
                field.data_type(),
                Int8 | Int16
                    | Int32
                    | Int64
                    | UInt8
                    | UInt16
                    | UInt32
                    | UInt64
                    | Float32
                    | Float64
                    | Utf8
                    | LargeUtf8
                    | Date32
                    | Date64
                    | Time32(_)
                    | Time64(_)
                    | Timestamp(_, _)
            ),
        };
        if !supported {
            return Err(open_arrow_sink_core::Error::Unsupported(format!(
                "cannot create {:?} index on column {} with type {}",
                index.index_type,
                index.column,
                field.data_type()
            )));
        }
    }
    Ok(())
}
