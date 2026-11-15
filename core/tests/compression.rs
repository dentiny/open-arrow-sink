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
