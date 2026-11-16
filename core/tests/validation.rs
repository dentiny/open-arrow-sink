use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use open_arrow_sink_core::{Error, Ordering, SinkCapability, WriteMode, WriteOptions, validation};
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
    let schema = Schema::new(vec![
        Field::new("a.b", DataType::Int64, true),
        Field::new("a", DataType::Int64, true),
    ]);
    assert_eq!(validation::unique_column_index(&schema, "a.b"), Some(0));
    assert_eq!(validation::unique_column_index(&schema, "missing"), None);
    let ambiguous = Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("id", DataType::Int64, true),
    ]);
    assert_eq!(validation::unique_column_index(&ambiguous, "id"), None);
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
fn default_options_accept_sinks_without_an_input_order_guarantee() {
    let schema = Schema::new(vec![Field::new("id", DataType::Int64, false)]);
    let options = WriteOptions::default();
    assert_eq!(Ordering::default(), Ordering::Unordered);
    assert_eq!(options.ordering, Ordering::Unordered);

    for ordering in [Ordering::Unordered, Ordering::PreserveInput] {
        let capability = SinkCapability::default()
            .with_supported_modes(&[WriteMode::Create])
            .with_ordering(ordering);
        validation::validate_options(&schema, &options, capability.clone()).unwrap();
        let preserve_input = options.clone().with_ordering(Ordering::PreserveInput);
        let result = validation::validate_options(&schema, &preserve_input, capability);
        if ordering == Ordering::PreserveInput {
            result.unwrap();
        } else {
            assert!(matches!(result, Err(Error::Unsupported(_))));
        }
    }
}
