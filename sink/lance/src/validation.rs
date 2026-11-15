//! Configuration, destination, transaction, and writer-state validation.
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

pub(crate) fn validate_destination_path(
    storage: &open_arrow_sink_storage::Storage,
    url: &open_arrow_sink_storage::Url,
) -> Result<()> {
    if storage.path(url)?.trim_matches('/').is_empty() {
        return Err(error::empty_destination());
    }
    Ok(())
}

/// Preflight only; conditional marker creation still arbitrates concurrent opens.
pub(crate) async fn destination_exists(
    storage: &open_arrow_sink_storage::Storage,
    url: &open_arrow_sink_storage::Url,
) -> Result<bool> {
    let mut exact = url.clone();
    exact.path_segments_mut().unwrap().pop_if_empty();
    Ok(storage.exists(&exact).await?
        || storage.exists(url).await?
        || !storage.list(url).await?.is_empty())
}

pub(crate) async fn validate_fresh_destination(
    storage: &open_arrow_sink_storage::Storage,
    url: &open_arrow_sink_storage::Url,
) -> Result<()> {
    if destination_exists(storage, url).await? {
        return Err(error::destination_exists());
    }
    Ok(())
}

pub(crate) async fn validate_existing_dataset(
    builder: lance::dataset::builder::DatasetBuilder,
    schema: Option<&Schema>,
) -> Result<()> {
    let dataset = builder.load().await.map_err(error::backend)?;
    if let Some(schema) = schema
        && &Schema::from(dataset.schema()) != schema
    {
        return Err(error::schema_mismatch());
    }
    Ok(())
}

pub(crate) fn validate_transaction_compatibility(
    target: &lance::dataset::transaction::Transaction,
    incoming: &lance::dataset::transaction::Transaction,
) -> Result<()> {
    use lance::dataset::transaction::Operation;
    if target.read_version != incoming.read_version {
        return Err(error::different_dataset_versions());
    }
    if !matches!(
        (&target.operation, &incoming.operation),
        (Operation::Overwrite { .. }, Operation::Overwrite { .. })
            | (Operation::Append { .. }, Operation::Append { .. })
    ) {
        return Err(error::invalid_create_fragments());
    }
    Ok(())
}

pub(crate) fn validate_write_state(failed: bool) -> Result<()> {
    if failed {
        return Err(error::failed());
    }
    Ok(())
}

pub(crate) fn validate_fragment_write_state(failed: bool, sender_open: bool) -> Result<()> {
    if failed || !sender_open {
        return Err(error::failed());
    }
    Ok(())
}

pub(crate) fn write_fragments(
    operation: &mut lance::dataset::transaction::Operation,
) -> Result<&mut Vec<lance_table::format::Fragment>> {
    match operation {
        lance::dataset::transaction::Operation::Overwrite { fragments, .. }
        | lance::dataset::transaction::Operation::Append { fragments } => Ok(fragments),
        _ => Err(error::invalid_create_fragments()),
    }
}

pub(crate) fn validate_session_transaction(
    transaction: Option<lance::dataset::transaction::Transaction>,
) -> Result<lance::dataset::transaction::Transaction> {
    transaction.ok_or_else(error::empty_writer_session)
}
