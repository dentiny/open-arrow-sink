//! Configuration, destination, transaction, and writer-state validation.
use arrow_schema::{DataType, Schema};
use open_arrow_sink_core::{
    Error, IndexSpec, IndexType, Result, SinkCapability, WriteOptions, is_blob_field, validation,
};

use lance::index::DatasetIndexExt;
use lance_file::version::LanceFileVersion;

use crate::error;

pub(crate) fn validate_options(
    schema: &Schema,
    options: &WriteOptions,
    capability: SinkCapability,
) -> Result<Option<LanceFileVersion>> {
    validation::validate_options(schema, options, capability)?;
    let storage_version = parse_storage_version(options.storage_version.as_deref())?;
    crate::compression::validate_encoding(schema, &options.compression)?;
    validate_indexes(schema, &options.indexes)?;
    Ok(storage_version)
}

pub(crate) fn validate_indexes(schema: &Schema, indexes: &[IndexSpec]) -> Result<()> {
    use DataType::*;
    validation::validate_indexes(schema, indexes)?;
    for index in indexes {
        let field = schema
            .field_with_name(&index.column)
            .map_err(|_| error::missing_index_column(&index.column))?;
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
            return Err(error::unsupported_index(index, field.data_type()));
        }
    }
    Ok(())
}

pub(crate) async fn validate_existing_dataset(
    builder: lance::dataset::builder::DatasetBuilder,
    schema: Option<&Schema>,
    storage_version: Option<LanceFileVersion>,
) -> Result<lance::Dataset> {
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
    if let Some(requested) = storage_version
        && requested.resolve() != dataset.manifest().data_storage_format.version
    {
        return Err(error::storage_version_mismatch(
            requested,
            dataset.manifest().data_storage_format.version,
        ));
    }
    if let Some(schema) = schema
        && &Schema::from(dataset.schema()) != schema
    {
        return Err(Error::SchemaMismatch);
    }
    Ok(dataset)
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

pub(crate) fn validate_blob_schema(schema: &Schema) -> Result<()> {
    fn check(field: &arrow_schema::Field, top_level: bool) -> Result<()> {
        if is_blob_field(field) {
            if !top_level
                || !matches!(
                    field.data_type(),
                    DataType::LargeBinary
                        | DataType::Utf8
                        | DataType::LargeUtf8
                        | DataType::Utf8View
                )
            {
                return Err(Error::InvalidInput(format!(
                    "blob-marked column must be top-level binary or URL text: {}",
                    field.name()
                )));
            }
            return Ok(());
        }
        if crate::blob::is_native_blob(field) {
            let DataType::Struct(fields) = field.data_type() else {
                return Err(Error::Unsupported(
                    "use logical blob bytes or URI inputs".into(),
                ));
            };
            let ty = |name: &str| {
                fields
                    .iter()
                    .find(|field| field.name() == name)
                    .map(|field| field.data_type())
            };
            if ty("kind").is_some()
                || ty("data") != Some(&DataType::LargeBinary)
                || ty("uri") != Some(&DataType::Utf8)
            {
                return Err(Error::Unsupported(
                    "use logical blob bytes or URI inputs, not prepared descriptors".into(),
                ));
            }
        }
        match field.data_type() {
            DataType::Struct(fields) => {
                for child in fields {
                    check(child, false)?;
                }
            }
            DataType::List(child)
            | DataType::LargeList(child)
            | DataType::FixedSizeList(child, _)
            | DataType::Map(child, _) => check(child, false)?,
            _ => {}
        }
        Ok(())
    }
    for input in schema.fields() {
        check(input, true)?;
    }
    Ok(())
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

pub(crate) async fn validate_existing_indexes(
    dataset: &lance::Dataset,
    indexes: &[IndexSpec],
) -> open_arrow_sink_core::Result<()> {
    let existing = dataset.load_indices().await.map_err(Error::backend)?;
    for spec in indexes {
        if spec.existing == open_arrow_sink_core::ExistingIndexPolicy::Error
            && existing
                .iter()
                .any(|i| i.name == crate::index::index_name(spec))
        {
            return Err(Error::AlreadyExists(format!(
                "index {} already exists; select Keep or Rebuild",
                crate::index::index_name(spec),
            )));
        }
    }
    Ok(())
}

pub(crate) fn validate_destination_path(path: &open_arrow_sink_storage::ObjectPath) -> Result<()> {
    if path.is_root() {
        return Err(error::invalid_destination_root());
    }
    Ok(())
}

/// Validate and return an existing dataset for reuse by subsequent checks.
/// None means this session will create a new destination.
pub(crate) async fn validate_destination(
    destination: &crate::destination::LanceDestination,
    schema: &open_arrow_sink_core::SchemaRef,
    mode: open_arrow_sink_core::WriteMode,
    storage_version: Option<LanceFileVersion>,
) -> Result<Option<lance::Dataset>> {
    use open_arrow_sink_core::WriteMode;
    use open_arrow_sink_storage::DirectoryState;
    if mode == WriteMode::Append {
        return validate_existing_dataset(
            destination.dataset_builder(),
            Some(schema),
            storage_version,
        )
        .await
        .map(Some);
    }
    if destination
        .storage
        .inspect_directory(&destination.path)
        .await?
        == DirectoryState::Missing
    {
        return Ok(None);
    }
    if mode == WriteMode::Create {
        return Err(Error::AlreadyExists(destination.url.to_string()));
    }
    validate_existing_dataset(destination.dataset_builder(), None, None)
        .await
        .map(Some)
}

pub(crate) async fn validate_index_requests(
    existing: Option<&lance::Dataset>,
    options: &WriteOptions,
) -> Result<()> {
    if options.mode == open_arrow_sink_core::WriteMode::Append
        && !options.indexes.is_empty()
        && let Some(dataset) = existing
    {
        validate_existing_indexes(dataset, &options.indexes).await?;
    }
    Ok(())
}
