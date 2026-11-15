//! Configuration, destination, writer-state, and transaction validation.
use std::path::Path;

use arrow_schema::{DataType, Schema};
use lance::{
    Dataset,
    dataset::transaction::{Operation, Transaction},
};
use open_arrow_sink_core::{
    Compression, Error, IndexSpec, IndexType, Result, SinkCapability, WriteOptions,
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

pub(crate) fn validate_path(path: &Path) -> Result<&str> {
    path.to_str().ok_or_else(error::invalid_path)
}

pub(crate) async fn validate_existing_dataset(uri: &str, schema: Option<&Schema>) -> Result<()> {
    let dataset = Dataset::open(uri).await.map_err(error::backend)?;
    if let Some(schema) = schema
        && &Schema::from(dataset.schema()) != schema
    {
        return Err(Error::SchemaMismatch);
    }
    Ok(())
}

pub(crate) fn validate_writer_state(failed: bool, active: bool) -> Result<()> {
    if failed || !active {
        return Err(error::failed());
    }
    Ok(())
}

pub(crate) fn validate_transaction_compatibility(
    target: &Transaction,
    incoming: &Transaction,
) -> Result<()> {
    if target.read_version != incoming.read_version {
        return Err(Error::Unsupported(
            "Lance writers observed different dataset versions".into(),
        ));
    }
    if !matches!(
        (&target.operation, &incoming.operation),
        (Operation::Overwrite { .. }, Operation::Overwrite { .. })
            | (Operation::Append { .. }, Operation::Append { .. })
    ) {
        return Err(Error::Unsupported(
            "expected compatible uncommitted Lance write fragments".into(),
        ));
    }
    Ok(())
}

pub(crate) fn validate_session_transaction(
    transaction: Option<Transaction>,
) -> Result<Transaction> {
    transaction.ok_or_else(|| Error::Unsupported("empty Lance writer session".into()))
}

pub(crate) fn validate_writer_resource<T>(resource: Option<T>) -> Result<T> {
    resource.ok_or_else(error::failed)
}
