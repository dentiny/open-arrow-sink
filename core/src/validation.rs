//! Mandatory shared contracts, separate from optional business validation layers.
use std::collections::HashSet;

use crate::error::{
    duplicate_index, invalid_index_column, invalid_target_file_size, preserve_input_unsupported,
    unsupported_index, unsupported_partition,
};
use crate::{
    CompressionCapability, CompressionOptions, Error, IndexSpec, IndexType, Ordering,
    PartitionOptions, PartitionType, RecordBatch, Result, SinkCapability, WriteOptions,
};
use arrow_schema::Schema;

/// Check declared capabilities and compression column names before destination I/O.
/// Backends must additionally validate codec-specific parameters, such as Zstd levels.
pub fn validate_options(
    schema: &Schema,
    options: &WriteOptions,
    capability: SinkCapability,
) -> Result<()> {
    validate_mode_options(options.mode, capability.supported_modes)?;
    if options.blob_handle.is_some() && !capability.supports_blob_handle {
        return Err(Error::Unsupported("blob handle configuration".into()));
    }
    validate_storage_version_options(
        options.storage_version.as_deref(),
        capability.supports_storage_version,
    )?;
    validate_compression_options(schema, &options.compression, capability.compression)?;
    validate_partition_options(options.partition, &capability.supported_partitions)?;
    validate_index_options(&options.indexes, capability.supported_indexes)?;
    validate_target_file_size_options(options.target_file_size)?;
    validate_ordering_options(options.ordering, capability.ordering)?;
    Ok(())
}

/// Reject unsupported modes, including the default, before destination I/O.
pub fn validate_mode_options(
    mode: crate::WriteMode,
    supported_modes: &[crate::WriteMode],
) -> Result<()> {
    if !supported_modes.contains(&mode) {
        return Err(Error::Unsupported(format!("write mode: {mode:?}")));
    }
    Ok(())
}

/// Check compression scopes, codecs, and exact top-level column names.
/// Backends additionally validate levels.
pub fn validate_compression_options(
    schema: &Schema,
    options: &CompressionOptions,
    capability: CompressionCapability,
) -> Result<()> {
    if let Some(compression) = options.global
        && !capability.global.contains(&compression.codec())
    {
        return Err(Error::Unsupported(format!(
            "global compression: {compression:?}"
        )));
    }
    for (column, compression) in &options.columns {
        if !capability.per_column.contains(&compression.codec()) {
            return Err(Error::Unsupported(format!(
                "compression {compression:?} on column {column}"
            )));
        }
    }
    if let Some(encoding) = options.structural_encoding
        && !capability.structural_encodings.contains(&encoding)
    {
        return Err(Error::Unsupported(format!(
            "structural encoding: {encoding:?}"
        )));
    }
    for (column, encoding) in &options.column_encodings {
        if !capability.structural_encodings.contains(encoding) {
            return Err(Error::Unsupported(format!(
                "structural encoding {encoding:?} on column {column}"
            )));
        }
    }
    for (path, compression) in &options.nested_columns {
        if !capability.nested_columns || !capability.per_column.contains(&compression.codec()) {
            return Err(Error::Unsupported(format!("nested compression: {path:?}")));
        }
        if path.len() < 2 || nested_field(schema, path).is_none() {
            return Err(Error::Unsupported(format!(
                "nested compression path must identify one field: {path:?}"
            )));
        }
    }
    for column in options
        .columns
        .keys()
        .chain(options.column_encodings.keys())
    {
        if unique_column_index(schema, column).is_none() {
            return Err(Error::Unsupported(format!(
                "compression/encoding column must identify one top-level field: {column}"
            )));
        }
    }
    Ok(())
}

/// Check that the sink supports the requested partition transform.
pub fn validate_partition_options(
    options: Option<PartitionOptions>,
    supported_partitions: &[PartitionType],
) -> Result<()> {
    if let Some(partition) = options
        && !supported_partitions.contains(&partition.partition_type)
    {
        return Err(unsupported_partition(partition.partition_type));
    }
    Ok(())
}

/// Check index types. Schema and column-type checks are performed separately.
pub fn validate_index_options(
    indexes: &[IndexSpec],
    supported_indexes: &[IndexType],
) -> Result<()> {
    for index in indexes {
        if !supported_indexes.contains(&index.index_type) {
            return Err(unsupported_index(index));
        }
    }
    Ok(())
}

/// Check that an explicit target file size is positive.
pub fn validate_target_file_size_options(target_file_size: Option<usize>) -> Result<()> {
    if target_file_size == Some(0) {
        return Err(invalid_target_file_size());
    }
    Ok(())
}

/// Check that the sink can provide the requested input-order guarantee.
pub fn validate_ordering_options(ordering: Ordering, supported_ordering: Ordering) -> Result<()> {
    if ordering == Ordering::PreserveInput && supported_ordering != Ordering::PreserveInput {
        return Err(preserve_input_unsupported());
    }
    Ok(())
}

/// Exact schema equality, including field order, nullability and metadata.
/// Casting, schema reconciliation and evolution require an explicit adapter.
pub fn validate_batch(schema: &Schema, batch: &RecordBatch) -> Result<()> {
    // Batches commonly retain the writer's Arc<Schema>. Avoid both cloning
    // that Arc and comparing metadata when the immutable schema is identical.
    let batch_schema = batch.schema_ref().as_ref();
    if !std::ptr::eq(schema, batch_schema) && schema != batch_schema {
        return Err(Error::SchemaMismatch);
    }
    Ok(())
}

/// Resolve an exact, unique top-level field name without interpreting dots as paths.
/// Return `None` for missing or ambiguous names. Callers can add operation-specific
/// error context before using the returned index.
pub fn unique_column_index(schema: &Schema, name: &str) -> Option<usize> {
    let mut matches = schema
        .fields()
        .iter()
        .enumerate()
        .filter(|(_, field)| field.name() == name);
    let (index, _) = matches.next()?;
    if matches.next().is_some() {
        return None;
    }
    Some(index)
}

/// Validate index columns and uniqueness of (column, index type) requests.
/// Concrete backends additionally validate their supported column types.
pub fn validate_indexes(schema: &Schema, indexes: &[IndexSpec]) -> Result<()> {
    let mut requests = HashSet::new();
    for index in indexes {
        if !requests.insert((&index.column, index.index_type)) {
            return Err(duplicate_index(index));
        }
        unique_column_index(schema, &index.column)
            .ok_or_else(|| invalid_index_column(&index.column))?;
    }
    Ok(())
}

/// Reject explicit storage versions when the backend does not support them.
/// Supporting backends additionally validate their version syntax.
pub fn validate_storage_version_options(
    storage_version: Option<&str>,
    supports_storage_version: bool,
) -> Result<()> {
    if storage_version.is_some() && !supports_storage_version {
        return Err(Error::Unsupported("storage version".into()));
    }
    Ok(())
}

/// Resolve literal struct names through list containers without list-item segments.
/// Native blob internals are not user-addressable column paths.
pub fn nested_field<'a>(schema: &'a Schema, path: &[String]) -> Option<&'a arrow_schema::Field> {
    fn fields<'a>(
        fields: &'a arrow_schema::Fields,
        path: &[String],
    ) -> Option<&'a arrow_schema::Field> {
        let mut matches = fields.iter().filter(|f| f.name() == &path[0]);
        let field = matches.next()?;
        if matches.next().is_some() {
            return None;
        }
        if path.len() == 1 {
            return Some(field);
        }
        if field
            .metadata()
            .get("ARROW:extension:name")
            .map(String::as_str)
            == Some("lance.blob.v2")
        {
            return None;
        }
        descend(field.data_type(), &path[1..])
    }
    fn descend<'a>(
        ty: &'a arrow_schema::DataType,
        path: &[String],
    ) -> Option<&'a arrow_schema::Field> {
        use arrow_schema::DataType;
        match ty {
            DataType::Struct(children) => fields(children, path),
            DataType::List(item) | DataType::LargeList(item) | DataType::FixedSizeList(item, _) => {
                descend(item.data_type(), path)
            }
            _ => None,
        }
    }
    if path.is_empty() {
        None
    } else {
        fields(schema.fields(), path)
    }
}
