//! Index configuration validation before destination creation.
use arrow_schema::{DataType, Schema};
use open_arrow_sink_core::{IndexSpec, IndexType, Result, validation};

use crate::error::{missing_index_column, unsupported_index};

pub(crate) fn validate_indexes(schema: &Schema, indexes: &[IndexSpec]) -> Result<()> {
    use DataType::*;
    validation::validate_indexes(schema, indexes)?;
    for index in indexes {
        let field = schema
            .field_with_name(&index.column)
            .map_err(|_| missing_index_column(&index.column))?;
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
            return Err(unsupported_index(index, field.data_type()));
        }
    }
    Ok(())
}
