//! Mandatory shared contracts, separate from optional business validation layers.
use std::collections::HashSet;

use crate::{
    CompressionCapability, CompressionOptions, Error, IndexSpec, IndexType, Ordering,
    PartitionOptions, PartitionType, RecordBatch, Result, SinkCapability, WriteOptions,
};
use arrow_schema::{Fields, Schema};

/// Check declared capabilities and compression column names before destination I/O.
/// Backends must additionally validate codec-specific parameters, such as Zstd levels.
pub fn validate_options(
    schema: &Schema,
    options: &WriteOptions,
    capability: SinkCapability,
) -> Result<()> {
    match option_errors(schema, options, capability)
        .into_iter()
        .next()
    {
        Some((_, _, error)) => Err(error),
        None => Ok(()),
    }
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
            return Err(Error::InvalidConfig(format!(
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
            return Err(Error::InvalidConfig(format!(
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
        return Err(Error::Unsupported(format!(
            "partitioned writes with {:?}",
            partition.partition_type
        )));
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
            return Err(Error::Unsupported(format!(
                "{:?} index construction on column {}",
                index.index_type, index.column
            )));
        }
    }
    Ok(())
}

/// Check that an explicit target file size is positive.
pub fn validate_target_file_size_options(target_file_size: Option<usize>) -> Result<()> {
    if target_file_size == Some(0) {
        return Err(Error::InvalidConfig(
            "target_file_size must be greater than zero".into(),
        ));
    }
    Ok(())
}

/// Check that the sink can provide the requested input-order guarantee.
pub fn validate_ordering_options(ordering: Ordering, supported_ordering: Ordering) -> Result<()> {
    if ordering == Ordering::PreserveInput && supported_ordering != Ordering::PreserveInput {
        return Err(Error::Unsupported("preserving input row order".into()));
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
    unique_field_index(schema.fields(), name)
}

/// Resolve an exact, unique name among sibling fields, including struct children.
/// Dots are literal characters, not path separators. Return `None` for missing
/// or ambiguous names; callers retain their operation-specific error context.
pub fn unique_field_index(fields: &Fields, name: &str) -> Option<usize> {
    let mut matches = fields
        .iter()
        .enumerate()
        .filter(|(_, field)| field.name() == name);
    let (index, _) = matches.next()?;
    if matches.next().is_some() {
        return None;
    }
    Some(index)
}

/// Validate index column existence and uniqueness independently of the backend.
/// Concrete backends additionally validate their supported column types.
pub fn validate_indexes(schema: &Schema, indexes: &[IndexSpec]) -> Result<()> {
    let mut columns = HashSet::new();
    for index in indexes {
        if !columns.insert(&index.column) {
            return Err(Error::InvalidConfig(format!(
                "multiple indexes requested for column {}",
                index.column
            )));
        }
        unique_column_index(schema, &index.column).ok_or_else(|| {
            Error::InvalidConfig(format!(
                "index column must identify one top-level field: {}",
                index.column
            ))
        })?;
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
pub fn nested_field<'a>(schema: &'a Schema, path: &[String]) -> Option<&'a arrow_schema::Field> {
    fn fields<'a>(
        fields: &'a arrow_schema::Fields,
        path: &[String],
    ) -> Option<&'a arrow_schema::Field> {
        let field = &fields[unique_field_index(fields, &path[0])?];
        if path.len() == 1 {
            return Some(field);
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

/// Collect independent shared-option failures, preserving the same validators as
/// `validate_options`. Compression entries and index requests are checked separately.
pub fn check_options(
    schema: &Schema,
    options: &WriteOptions,
    capability: SinkCapability,
) -> Vec<crate::SchemaIssue> {
    option_errors(schema, options, capability)
        .into_iter()
        .map(|(path, kind, error)| crate::SchemaIssue::new(path, kind, error.to_string()))
        .collect()
}

fn option_errors(
    schema: &Schema,
    options: &WriteOptions,
    capability: SinkCapability,
) -> Vec<(Vec<String>, crate::SchemaIssueKind, Error)> {
    use crate::SchemaIssueKind as Kind;
    let mut issues = Vec::new();
    let mut record = |path: Vec<String>, result: Result<()>| {
        if let Err(error) = result {
            let kind = if matches!(&error, Error::InvalidConfig(_)) && !path.is_empty() {
                Kind::MissingColumn
            } else {
                Kind::InvalidOption
            };
            issues.push((path, kind, error));
        }
    };
    record(
        vec![],
        validate_mode_options(options.mode, capability.supported_modes),
    );
    if options.blob_handle.is_some() && !capability.supports_blob_handle {
        record(
            vec![],
            Err(Error::Unsupported("blob handle configuration".into())),
        );
    }
    if (options.upload.part_size.is_some() || options.upload.concurrency.is_some())
        && !capability.supports_upload_options
    {
        record(
            vec![],
            Err(Error::Unsupported("upload part size or concurrency".into())),
        );
    }
    record(
        vec![],
        validate_storage_version_options(
            options.storage_version.as_deref(),
            capability.supports_storage_version,
        ),
    );
    record(
        vec![],
        validate_partition_options(options.partition, &capability.supported_partitions),
    );
    record(
        vec![],
        validate_target_file_size_options(options.target_file_size),
    );
    record(
        vec![],
        validate_ordering_options(options.ordering, capability.ordering),
    );
    for index in &options.indexes {
        record(
            vec![index.column.clone()],
            validate_index_options(std::slice::from_ref(index), capability.supported_indexes),
        );
    }
    // Split scopes so one invalid codec cannot hide another column or encoding.
    let compression = &options.compression;
    for single in [
        CompressionOptions {
            global: compression.global,
            ..Default::default()
        },
        CompressionOptions {
            structural_encoding: compression.structural_encoding,
            ..Default::default()
        },
    ] {
        record(
            vec![],
            validate_compression_options(schema, &single, capability.compression),
        );
    }
    for (column, codec) in &compression.columns {
        let single = CompressionOptions {
            columns: [(column.clone(), *codec)].into(),
            ..Default::default()
        };
        record(
            vec![column.clone()],
            validate_compression_options(schema, &single, capability.compression),
        );
    }
    for (column, encoding) in &compression.column_encodings {
        let single = CompressionOptions {
            column_encodings: [(column.clone(), *encoding)].into(),
            ..Default::default()
        };
        record(
            vec![column.clone()],
            validate_compression_options(schema, &single, capability.compression),
        );
    }
    for (path, codec) in &compression.nested_columns {
        let single = CompressionOptions {
            nested_columns: [(path.clone(), *codec)].into(),
            ..Default::default()
        };
        record(
            path.clone(),
            validate_compression_options(schema, &single, capability.compression),
        );
    }
    // Existence is independent of codec support, so always report it as well.
    for column in compression
        .columns
        .keys()
        .chain(compression.column_encodings.keys())
    {
        if unique_column_index(schema, column).is_none()
            && !issues.iter().any(|(path, kind, _)| {
                path == &vec![column.clone()] && *kind == Kind::MissingColumn
            })
        {
            issues.push((
                vec![column.clone()],
                Kind::MissingColumn,
                Error::InvalidConfig(format!(
                    "compression/encoding column must identify one top-level field: {column}"
                )),
            ));
        }
    }
    for path in compression.nested_columns.keys() {
        if (path.len() < 2 || nested_field(schema, path).is_none())
            && !issues
                .iter()
                .any(|(existing, kind, _)| existing == path && *kind == Kind::MissingColumn)
        {
            issues.push((
                path.clone(),
                Kind::MissingColumn,
                Error::InvalidConfig(format!(
                    "nested compression path must identify one field: {path:?}"
                )),
            ));
        }
    }
    issues
}
