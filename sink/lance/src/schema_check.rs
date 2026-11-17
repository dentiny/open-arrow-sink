//! Destination-independent Lance preflight shared by UI callers and open.
use arrow_schema::{DataType, Field, Fields, Schema};
use open_arrow_sink_core::{
    CompressionOptions, SchemaIssue, SchemaIssueKind as Kind, SinkCapability, WriteOptions,
    is_blob_field, validation,
};
use std::{collections::HashSet, sync::Arc};

pub(crate) fn check(
    schema: &Schema,
    options: &WriteOptions,
    capability: SinkCapability,
) -> Vec<SchemaIssue> {
    let mut issues = check_fields(schema);
    issues.extend(validation::check_options(
        schema,
        options,
        capability.clone(),
    ));
    if let Err(error) = crate::validation::parse_storage_version(options.storage_version.as_deref())
    {
        issues.push(SchemaIssue::new(
            vec![],
            Kind::InvalidOption,
            error.to_string(),
        ));
    }
    let mut indexes = HashSet::new();
    for index in &options.indexes {
        let path = vec![index.column.clone()];
        if !indexes.insert(&index.column) {
            issues.push(SchemaIssue::new(
                path.clone(),
                Kind::Duplicate,
                "multiple indexes requested for column",
            ));
        }
        match validation::unique_column_index(schema, &index.column) {
            None => issues.push(SchemaIssue::new(
                path,
                Kind::MissingColumn,
                "index column must exist and be unique",
            )),
            Some(i) => {
                let field = schema.field(i);
                // Blob-marked text is persisted as a blob, not as searchable text.
                if is_blob_field(field)
                    || crate::blob::is_native_blob(field)
                    || !crate::validation::index_eligible(index.index_type, field.data_type())
                {
                    issues.push(SchemaIssue::new(
                        path,
                        Kind::IndexType,
                        format!(
                            "unsupported {:?} index on {:?}",
                            index.index_type,
                            field.data_type()
                        ),
                    ));
                }
            }
        }
    }
    let compression = &options.compression;
    for (path, codec) in compression
        .global
        .iter()
        .map(|c| (vec![], c))
        .chain(
            compression
                .columns
                .iter()
                .map(|(name, c)| (vec![name.clone()], c)),
        )
        .chain(
            compression
                .nested_columns
                .iter()
                .map(|(path, c)| (path.clone(), c)),
        )
    {
        if !capability.compression.per_column.contains(&codec.codec()) {
            continue; // Shared preflight already reports unsupported codecs.
        }
        if let Err(error) = crate::compression::native_compression(*codec) {
            issues.push(SchemaIssue::new(
                path,
                Kind::InvalidOption,
                error.to_string(),
            ));
        }
    }
    // Check encoding restrictions independently for every column and nested path.
    for field in schema.fields() {
        let single = CompressionOptions {
            structural_encoding: compression
                .column_encodings
                .get(field.name())
                .copied()
                .or(compression.structural_encoding),
            ..Default::default()
        };
        let one = Schema::new(vec![field.clone()]);
        if let Err(error) = crate::compression::validate_encoding(&one, &single) {
            issues.push(SchemaIssue::new(
                vec![field.name().clone()],
                Kind::InvalidOption,
                error.to_string(),
            ));
        }
    }
    for (path, codec) in &compression.nested_columns {
        let single = CompressionOptions {
            nested_columns: [(path.clone(), *codec)].into(),
            ..Default::default()
        };
        if let Err(error) = crate::compression::validate_encoding(schema, &single) {
            issues.push(SchemaIssue::new(
                path.clone(),
                Kind::InvalidOption,
                error.to_string(),
            ));
        }
    }
    // Keep conversion in the preflight so new SDK checks cannot first run after I/O.
    if issues.is_empty() {
        match crate::write_options::LanceSchemas::new(Arc::new(schema.clone()), options) {
            Err(error) => issues.push(SchemaIssue::new(
                vec![],
                Kind::InvalidOption,
                error.to_string(),
            )),
            Ok(schemas) => {
                if let Err(error) = lance::datatypes::Schema::try_from(schemas.logical.as_ref()) {
                    issues.push(SchemaIssue::new(
                        vec![],
                        Kind::UnsupportedType,
                        error.to_string(),
                    ));
                }
            }
        }
    }
    issues
}

pub(crate) fn check_fields(schema: &Schema) -> Vec<SchemaIssue> {
    let mut issues = Vec::new();
    fields(schema.fields(), &[], true, &mut issues);
    issues
}

fn fields(children: &Fields, parent: &[String], top_level: bool, issues: &mut Vec<SchemaIssue>) {
    let mut names = HashSet::new();
    for child in children {
        let mut path = parent.to_vec();
        path.push(child.name().clone());
        if !names.insert(child.name()) {
            issues.push(SchemaIssue::new(
                path.clone(),
                Kind::Duplicate,
                "duplicate sibling field name",
            ));
        }
        field(child, &path, top_level, issues);
    }
}

fn field(input: &Field, path: &[String], top_level: bool, issues: &mut Vec<SchemaIssue>) {
    if input.name().is_empty() || input.name().contains('.') {
        issues.push(SchemaIssue::new(
            path.to_vec(),
            Kind::InvalidName,
            format!(
                "{:?} is not a valid Lance column name: empty names and dots are forbidden",
                input.name()
            ),
        ));
    }
    if matches!(
        input.name().as_str(),
        "_rowid"
            | "_rowaddr"
            | "_rowoffset"
            | "_row_last_updated_at_version"
            | "_row_created_at_version"
    ) {
        issues.push(SchemaIssue::new(
            path.to_vec(),
            Kind::ReservedName,
            format!(
                "{:?} is not a valid Lance column name: reserved name",
                input.name()
            ),
        ));
    }
    if input
        .metadata()
        .get("ARROW:extension:name")
        .is_some_and(|name| name == "arrow.parquet.variant")
    {
        issues.push(SchemaIssue::new(
            path.to_vec(),
            Kind::UnsupportedType,
            "Parquet VARIANT is not supported by Lance",
        ));
    }
    if is_blob_field(input) || crate::blob::is_native_blob(input) {
        if is_blob_field(input) && !top_level {
            issues.push(SchemaIssue::new(
                path.to_vec(),
                Kind::BlobType,
                "blob-marked columns must be top-level",
            ));
        } else if let Err(error) =
            crate::validation::validate_blob_schema(&Schema::new(vec![input.clone()]))
        {
            issues.push(SchemaIssue::new(
                path.to_vec(),
                Kind::BlobType,
                error.to_string(),
            ));
        }
    }
    let before_children = issues.len();
    match input.data_type() {
        DataType::Struct(children) => fields(children, path, false, issues),
        DataType::List(child)
        | DataType::LargeList(child)
        | DataType::ListView(child)
        | DataType::LargeListView(child)
        | DataType::FixedSizeList(child, _)
        | DataType::Map(child, _) => {
            let mut child_path = path.to_vec();
            child_path.push(child.name().clone());
            field(child, &child_path, false, issues);
        }
        DataType::Union(children, _) => {
            fields(
                &children
                    .iter()
                    .map(|(_, child)| child.clone())
                    .collect::<Fields>(),
                path,
                false,
                issues,
            );
        }
        DataType::RunEndEncoded(run_ends, values) => {
            fields(
                &vec![run_ends.clone(), values.clone()].into(),
                path,
                false,
                issues,
            );
        }
        _ => {}
    }
    // SDK field conversion is the type authority. A descendant's name error
    // must not hide an independent unsupported container type or metadata error.
    if let Err(error) = lance::datatypes::Field::try_from(input) {
        let message = error.to_string();
        if !issues[before_children..]
            .iter()
            .any(|issue| issue.kind == Kind::UnsupportedType && issue.message == message)
        {
            issues.push(SchemaIssue::new(
                path.to_vec(),
                Kind::UnsupportedType,
                message,
            ));
        }
    }
    if matches!(input.data_type(), DataType::FixedSizeList(_, size) if *size <= 0) {
        issues.push(SchemaIssue::new(
            path.to_vec(),
            Kind::UnsupportedType,
            "fixed-size list dimension must be positive",
        ));
    }
}
