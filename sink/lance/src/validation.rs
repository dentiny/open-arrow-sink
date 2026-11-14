//! Index configuration validation before destination creation.
use std::collections::HashSet;

use arrow_schema::{DataType, Schema};
use open_arrow_sink_core::Result;

use crate::index::{LanceIndexKind, LanceIndexSpec};

pub(crate) fn validate_indexes(schema: &Schema, indexes: &[LanceIndexSpec]) -> Result<()> {
    use DataType::*;
    let mut seen = HashSet::new();
    for index in indexes {
        if !seen.insert(&index.column) {
            return Err(open_arrow_sink_core::Error::Unsupported(format!(
                "multiple indexes requested for column {}",
                index.column
            )));
        }
        let field = schema.field_with_name(&index.column).map_err(|_| {
            open_arrow_sink_core::Error::Unsupported(format!(
                "index column not found: {}",
                index.column
            ))
        })?;
        let supported = match index.kind {
            LanceIndexKind::Scalar => matches!(
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
            LanceIndexKind::Vector => {
                matches!(field.data_type(), FixedSizeList(child, dimension) if *dimension > 0 && matches!(child.data_type(), Float16 | Float32 | Float64))
            }
            LanceIndexKind::Text => matches!(field.data_type(), Utf8 | LargeUtf8 | Utf8View),
            LanceIndexKind::BloomFilter => matches!(
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
                index.kind,
                index.column,
                field.data_type()
            )));
        }
    }
    Ok(())
}
