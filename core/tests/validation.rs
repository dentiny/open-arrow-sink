use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use open_arrow_sink_core::{Error, Ordering, WriteMode, validation};
use std::{collections::HashMap, sync::Arc};

#[test]
fn batch_contract_includes_metadata_and_nullability() {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let batch =
        RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![1]))]).unwrap();
    validation::validate_batch(&schema, &batch).unwrap();
    let metadata = schema
        .as_ref()
        .clone()
        .with_metadata(HashMap::from([("contract".into(), "changed".into())]));
    assert!(matches!(
        validation::validate_batch(&metadata, &batch),
        Err(Error::SchemaMismatch)
    ));
    let nullable = Schema::new(vec![Field::new("id", DataType::Int64, true)]);
    assert!(matches!(
        validation::validate_batch(&nullable, &batch),
        Err(Error::SchemaMismatch)
    ));
}

#[test]
fn column_resolution_is_literal_and_rejects_ambiguity() {
    let field = |name| Field::new(name, DataType::Int64, true);
    for (names, name, expected) in [
        (vec![], "id", None),
        (vec!["a.b", "a"], "a.b", Some(0)),
        (vec!["a.b", "a"], "a", Some(1)),
        (vec!["id"], "missing", None),
        (vec!["id", "id"], "id", None),
        (vec!["id", "other", "id"], "id", None),
        (vec!["id", "id", "other"], "other", Some(2)),
    ] {
        let schema = Schema::new(names.into_iter().map(field).collect::<Vec<_>>());
        assert_eq!(validation::unique_column_index(&schema, name), expected);
        assert_eq!(
            validation::unique_field_index(schema.fields(), name),
            expected
        );
    }
}

#[test]
fn unsupported_guarantees_modes_and_zero_size_are_rejected() {
    assert!(matches!(
        validation::validate_mode_options(WriteMode::Create, &[WriteMode::Append]),
        Err(Error::Unsupported(_))
    ));
    assert!(
        validation::validate_ordering_options(Ordering::PreserveInput, Ordering::Unordered)
            .is_err()
    );
    validation::validate_ordering_options(Ordering::Unordered, Ordering::PreserveInput).unwrap();
    assert!(matches!(
        validation::validate_target_file_size_options(Some(0)),
        Err(Error::InvalidConfig(_))
    ));
    validation::validate_target_file_size_options(None).unwrap();
    validation::validate_target_file_size_options(Some(1)).unwrap();
}

#[test]
fn structured_options_collect_independent_failures() {
    use open_arrow_sink_core::{CompressionSpec, SchemaIssueKind, SinkCapability, WriteOptions};
    let schema = Schema::new(vec![Field::new("id", DataType::Int64, true)]);
    let mut options = WriteOptions::default();
    options.target_file_size = Some(0);
    options.storage_version = Some("2.2".into());
    options
        .compression
        .columns
        .insert("missing".into(), CompressionSpec::Snappy);
    let capability = SinkCapability::default().with_supported_modes(&[WriteMode::Create]);
    let issues = validation::check_options(&schema, &options, capability.clone());
    assert!(
        issues
            .iter()
            .any(|issue| issue.message.contains("target_file_size"))
    );
    assert!(
        issues
            .iter()
            .any(|issue| issue.message.contains("storage version"))
    );
    assert!(
        issues
            .iter()
            .any(|issue| issue.path == ["missing"] && issue.kind == SchemaIssueKind::MissingColumn)
    );
    assert!(validation::validate_options(&schema, &options, capability).is_err());
}
