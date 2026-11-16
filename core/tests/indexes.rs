use arrow_schema::{DataType, Field, Schema};
use open_arrow_sink_core::{ExistingIndexPolicy, IndexSpec, IndexType, validation};

fn schema() -> Schema {
    Schema::new(vec![Field::new("text", DataType::Utf8, false)])
}

#[test]
fn distinct_index_types_can_share_a_column() {
    let requests = [
        IndexSpec::new("text", IndexType::Scalar),
        IndexSpec::new("text", IndexType::Text),
        IndexSpec::new("text", IndexType::BloomFilter),
    ];
    validation::validate_indexes(&schema(), &requests).unwrap();
}

#[test]
fn duplicate_type_is_rejected_even_with_different_existing_policy() {
    let requests = [
        IndexSpec::new("text", IndexType::Scalar),
        IndexSpec::new("text", IndexType::Scalar)
            .with_existing_policy(ExistingIndexPolicy::Rebuild),
    ];
    let error = validation::validate_indexes(&schema(), &requests).unwrap_err();
    assert!(error.to_string().contains("duplicate Scalar index"));
}

#[test]
fn different_types_do_not_bypass_missing_or_ambiguous_column_checks() {
    let requests = [
        IndexSpec::new("missing", IndexType::Scalar),
        IndexSpec::new("missing", IndexType::Text),
    ];
    assert!(validation::validate_indexes(&schema(), &requests).is_err());
    let schema = Schema::new(vec![
        Field::new("text", DataType::Utf8, false),
        Field::new("text", DataType::Utf8, false),
    ]);
    assert!(
        validation::validate_indexes(&schema, &[IndexSpec::new("text", IndexType::Scalar)])
            .is_err()
    );
}
