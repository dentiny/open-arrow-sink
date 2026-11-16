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
