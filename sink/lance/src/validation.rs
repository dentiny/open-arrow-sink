//! Configuration, destination, transaction, and writer-state validation.
use arrow_schema::{DataType, Schema};
use open_arrow_sink_core::{
    Compression, CompressionOptions, Error, IndexSpec, IndexType, Result, SinkCapability,
    WriteOptions, ZSTD_COMPRESSION_LEVEL_RANGE, is_blob_field, validation,
};

use crate::error;

pub(crate) fn validate_options(
    schema: &Schema,
    options: &WriteOptions,
    capability: SinkCapability,
) -> Result<()> {
    validation::validate_options(schema, options, capability)?;
    parse_storage_version(options.storage_version.as_deref())?;
    validate_encoding(schema, &options.compression)?;
    validate_compression(&options.compression)?;
    validate_indexes(schema, &options.indexes)
}

fn validate_compression(options: &CompressionOptions) -> Result<()> {
    for compression in options
        .global
        .iter()
        .chain(options.columns.values())
        .chain(options.nested_columns.values())
    {
        // Lance accepts 0 as the native Zstd default, in addition to positive levels.
        if let Compression::Zstd { level: Some(level) } = compression
            && *level != 0
            && !ZSTD_COMPRESSION_LEVEL_RANGE.contains(level)
        {
            return Err(Error::InvalidConfig(format!(
                "Lance Zstd compression level must be between 0 and {}: {level}",
                ZSTD_COMPRESSION_LEVEL_RANGE.end()
            )));
        }
    }
    Ok(())
}

fn validate_encoding(schema: &Schema, options: &CompressionOptions) -> Result<()> {
    // Native blob descriptors are opaque to Lance users. Keep this backend rule
    // here rather than teaching core's Arrow path resolver about Lance metadata.
    for path in options.nested_columns.keys() {
        if (1..path.len()).any(|end| {
            validation::nested_field(schema, &path[..end]).is_some_and(|field| {
                field
                    .metadata()
                    .get("ARROW:extension:name")
                    .map(String::as_str)
                    == Some("lance.blob.v2")
            })
        }) {
            return Err(Error::Unsupported(format!(
                "nested compression inside native blob column: {path:?}"
            )));
        }
    }
    for field in schema.fields() {
        let encoding = options
            .column_encodings
            .get(field.name())
            .copied()
            .or(options.structural_encoding);
        if encoding.is_some()
            && (is_blob_field(field)
                || field
                    .metadata()
                    .get("ARROW:extension:name")
                    .map(String::as_str)
                    == Some("lance.blob.v2"))
        {
            return Err(Error::Unsupported(format!(
                "structural encoding on blob column {}",
                field.name()
            )));
        }
    }
    Ok(())
}

pub(crate) fn validate_indexes(schema: &Schema, indexes: &[IndexSpec]) -> Result<()> {
    use DataType::*;
    validation::validate_indexes(schema, indexes)?;
    for index in indexes {
        let field = schema.field_with_name(&index.column).map_err(|_| {
            Error::InvalidConfig(format!("index column not found: {}", index.column))
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
            return Err(Error::Unsupported(format!(
                "cannot create {:?} index on column {} with type {}",
                index.index_type,
                index.column,
                field.data_type()
            )));
        }
    }
    Ok(())
}

pub(crate) async fn validate_existing_dataset(
    builder: lance::dataset::builder::DatasetBuilder,
    schema: Option<&Schema>,
    storage_version: Option<&str>,
) -> Result<()> {
    let dataset = builder.load().await.map_err(Error::backend)?;
    if !matches!(
        dataset.manifest().data_storage_format.version,
        lance_file::version::ConcreteFileVersion::V2_2
            | lance_file::version::ConcreteFileVersion::V2_3
    ) {
        return Err(error::unsupported_storage_version(
            dataset.manifest().data_storage_format.version,
        ));
    }
    if let Some(requested) = parse_storage_version(storage_version)?
        && requested.resolve() != dataset.manifest().data_storage_format.version
    {
        return Err(Error::InvalidConfig(format!(
            "Lance append storage version {requested} does not match existing dataset version {}",
            dataset.manifest().data_storage_format.version
        )));
    }
    if let Some(schema) = schema
        && &Schema::from(dataset.schema()) != schema
    {
        return Err(Error::SchemaMismatch);
    }
    Ok(())
}

pub(crate) fn validate_transaction_compatibility(
    target: &lance::dataset::transaction::Transaction,
    incoming: &lance::dataset::transaction::Transaction,
) -> Result<()> {
    use lance::dataset::transaction::Operation;
    if target.read_version != incoming.read_version {
        return Err(Error::InvalidInput(
            "Lance writers observed different dataset versions".into(),
        ));
    }
    if !matches!(
        (&target.operation, &incoming.operation),
        (Operation::Overwrite { .. }, Operation::Overwrite { .. })
            | (Operation::Append { .. }, Operation::Append { .. })
    ) {
        return Err(Error::InvalidInput(
            "expected compatible uncommitted Lance write fragments".into(),
        ));
    }
    Ok(())
}

pub(crate) fn write_fragments(
    operation: &mut lance::dataset::transaction::Operation,
) -> Result<&mut Vec<lance_table::format::Fragment>> {
    match operation {
        lance::dataset::transaction::Operation::Overwrite { fragments, .. }
        | lance::dataset::transaction::Operation::Append { fragments } => Ok(fragments),
        _ => Err(Error::InvalidInput(
            "expected compatible uncommitted Lance write fragments".into(),
        )),
    }
}

pub(crate) fn validate_session_transaction(
    transaction: Option<lance::dataset::transaction::Transaction>,
) -> Result<lance::dataset::transaction::Transaction> {
    transaction.ok_or_else(|| Error::InvalidInput("empty Lance writer session".into()))
}

pub(crate) fn validate_blob_schema(schema: &Schema) -> Result<()> {
    for field in schema.fields() {
        if is_blob_field(field) && field.data_type() != &DataType::LargeBinary {
            return Err(error::invalid_blob_column(field.name()));
        }
    }
    Ok(())
}
pub(crate) fn validate_parallel_blob_schema(schema: &Schema) -> Result<()> {
    validate_blob_schema(schema)?;
    if schema.fields().iter().any(|field| is_blob_field(field)) {
        return Err(Error::Unsupported(
            "parallel sessions do not support blob-marked columns".into(),
        ));
    }
    Ok(())
}
pub(crate) fn validate_blob_placements(
    batch: &open_arrow_sink_core::RecordBatch,
    blobs: Vec<open_arrow_sink_core::BlobPlacement>,
    session: &std::sync::Arc<crate::blob::BlobSession>,
) -> Result<std::collections::HashMap<(usize, usize), open_arrow_sink_core::BlobHandle>> {
    use crate::blob::NativeBlob;
    use arrow_array::{Array, LargeBinaryArray};
    let mut placements = std::collections::HashMap::new();
    for blob in blobs {
        let field = batch
            .schema_ref()
            .fields()
            .get(blob.column)
            .ok_or_else(|| Error::InvalidInput("blob column out of bounds".into()))?;
        if blob.row >= batch.num_rows() || !is_blob_field(field) {
            return Err(Error::InvalidInput(
                "blob placement must target a blob-marked cell".into(),
            ));
        }
        let array = batch
            .column(blob.column)
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .ok_or_else(|| error::invalid_blob_column(field.name()))?;
        if array.is_null(blob.row) || !array.value(blob.row).is_empty() {
            return Err(Error::InvalidInput(
                "blob placement must replace an empty non-null payload".into(),
            ));
        }
        let native = blob
            .blob
            .downcast_ref::<NativeBlob>()
            .ok_or_else(|| Error::InvalidInput("foreign blob handle".into()))?;
        if !std::sync::Arc::ptr_eq(&native.session, session) {
            return Err(Error::InvalidInput(
                "blob handle belongs to another session".into(),
            ));
        }
        if placements
            .insert((blob.column, blob.row), blob.blob)
            .is_some()
        {
            return Err(Error::InvalidInput("duplicate blob placement".into()));
        }
    }
    Ok(placements)
}

pub(crate) fn parse_storage_version(
    version: Option<&str>,
) -> Result<Option<lance_file::version::LanceFileVersion>> {
    use lance_file::version::LanceFileVersion;
    match version {
        None => Ok(None),
        Some("2.2") => Ok(Some(LanceFileVersion::V2_2)),
        Some("2.3") => Ok(Some(LanceFileVersion::V2_3)),
        Some(version) => Err(error::unsupported_storage_version(version)),
    }
}
