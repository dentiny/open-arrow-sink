use arrow_schema::{DataType, Field, Schema};
use open_arrow_sink_core::{IndexSpec, IndexType, validation};

#[test]
fn index_columns_must_resolve_to_exactly_one_top_level_field() {
    let indexes = [IndexSpec::new("id", IndexType::Scalar)];
    let unique = Schema::new(vec![Field::new("id", DataType::Int64, true)]);
    assert!(validation::validate_indexes(&unique, &indexes).is_ok());
    assert!(validation::validate_indexes(&Schema::empty(), &indexes).is_err());
    let ambiguous = Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("id", DataType::Utf8, true),
    ]);
    assert!(validation::validate_indexes(&ambiguous, &indexes).is_err());
}

#[test]
fn column_resolution_uses_literal_names_and_rejects_ambiguity() {
    let schema = Schema::new(vec![
        Field::new("a.b", DataType::Int64, true),
        Field::new("id", DataType::Int64, true),
        Field::new("id", DataType::Utf8, true),
    ]);
    assert_eq!(validation::unique_column_index(&schema, "a.b"), Some(0));
    assert_eq!(validation::unique_column_index(&schema, "a"), None);
    assert_eq!(validation::unique_column_index(&schema, "id"), None);
    assert_eq!(
        validation::unique_column_index(&Schema::empty(), "id"),
        None
    );
}

#[test]
fn compression_uses_the_same_unique_column_contract() {
    use open_arrow_sink_core::{
        Compression, CompressionCapability, CompressionCodec, CompressionOptions,
    };
    let options = CompressionOptions {
        columns: [("id".into(), Compression::Uncompressed)].into(),
        ..Default::default()
    };
    let capability = CompressionCapability {
        global: &[],
        per_column: &[CompressionCodec::Uncompressed],
    };
    let field = Field::new("id", DataType::Int64, true);
    assert!(
        validation::validate_compression_options(
            &Schema::new(vec![field.clone()]),
            &options,
            capability,
        )
        .is_ok()
    );
    assert!(
        validation::validate_compression_options(
            &Schema::new(vec![field.clone(), field]),
            &options,
            capability,
        )
        .is_err()
    );
    assert!(
        validation::validate_compression_options(&Schema::empty(), &options, capability,).is_err()
    );
}
