use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema};
use open_arrow_sink_core::{
    CompressionCapability, CompressionOptions, StructuralEncoding, validation,
};

#[test]
fn structural_encoding_checks_capabilities_and_unique_column_names() {
    let supported = CompressionCapability {
        structural_encodings: &[StructuralEncoding::Miniblock, StructuralEncoding::Fullzip],
        ..Default::default()
    };
    let schema = Schema::new(vec![Field::new("literal.dot", DataType::Utf8, false)]);
    let options = CompressionOptions {
        structural_encoding: Some(StructuralEncoding::Fullzip),
        column_encodings: [("literal.dot".into(), StructuralEncoding::Miniblock)].into(),
        ..Default::default()
    };
    validation::validate_compression_options(&schema, &options, supported).unwrap();
    assert!(
        validation::validate_compression_options(&schema, &options, Default::default()).is_err()
    );
    for schema in [
        Schema::empty(),
        Schema::new(vec![
            Arc::new(schema.field(0).clone()),
            Arc::new(schema.field(0).clone()),
        ]),
    ] {
        assert!(validation::validate_compression_options(&schema, &options, supported).is_err());
    }
}

#[test]
fn nested_paths_are_literal_and_cross_lists() {
    use open_arrow_sink_core::{Compression, CompressionCodec};
    let children = vec![Arc::new(Field::new("literal.dot", DataType::Utf8, false))].into();
    let schema = Schema::new(vec![Field::new(
        "items",
        DataType::List(Arc::new(Field::new(
            "item",
            DataType::Struct(children),
            false,
        ))),
        false,
    )]);
    let capability = CompressionCapability {
        per_column: &[CompressionCodec::Zstd],
        nested_columns: true,
        ..Default::default()
    };
    for (path, valid) in [
        (vec!["items", "literal.dot"], true),
        (vec!["items", "literal", "dot"], false),
        (vec!["items", "item", "literal.dot"], false),
        (vec!["items"], false),
    ] {
        let options = CompressionOptions {
            nested_columns: [(
                path.into_iter().map(String::from).collect(),
                Compression::Zstd { level: None },
            )]
            .into(),
            ..Default::default()
        };
        assert_eq!(
            validation::validate_compression_options(&schema, &options, capability).is_ok(),
            valid
        );
        assert!(
            validation::validate_compression_options(
                &schema,
                &options,
                CompressionCapability {
                    nested_columns: false,
                    ..capability
                }
            )
            .is_err()
        );
    }
}

#[test]
fn nested_paths_do_not_interpret_backend_extension_metadata() {
    let schema = Schema::new(vec![
        Field::new(
            "extension",
            DataType::Struct(vec![Arc::new(Field::new("value", DataType::Utf8, false))].into()),
            false,
        )
        .with_metadata([("ARROW:extension:name".into(), "lance.blob.v2".into())].into()),
    ]);
    let field = validation::nested_field(&schema, &["extension".into(), "value".into()]);
    assert_eq!(field.unwrap().name(), "value");
}
