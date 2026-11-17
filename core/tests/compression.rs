use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema};
use open_arrow_sink_core::{
    CompressionCapability, CompressionOptions, StructuralEncoding, validation,
};

#[test]
fn structural_encoding_checks_capabilities_and_unique_column_names() {
    let supported = CompressionCapability::default()
        .with_structural_encodings(&[StructuralEncoding::Miniblock, StructuralEncoding::Fullzip]);
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
    use open_arrow_sink_core::{CompressionCodec, CompressionSpec};
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
    let capability = CompressionCapability::default()
        .with_per_column(&[CompressionCodec::Zstd])
        .with_nested_columns(true);
    for (path, valid) in [
        (vec!["items", "literal.dot"], true),
        (vec!["items", "literal", "dot"], false),
        (vec!["items", "item", "literal.dot"], false),
        (vec!["items"], false),
    ] {
        let options = CompressionOptions {
            nested_columns: [(
                path.into_iter().map(String::from).collect(),
                CompressionSpec::Zstd { level: None },
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
                (capability).with_nested_columns(false)
            )
            .is_err()
        );
    }
}

#[test]
fn nested_paths_reject_missing_or_ambiguous_siblings() {
    use arrow_schema::Fields;
    let field = |name| Arc::new(Field::new(name, DataType::Utf8, true));
    for children in [
        Fields::empty(),
        vec![field("other")].into(),
        vec![field("value"), field("other"), field("value")].into(),
    ] {
        let nested = DataType::Struct(children);
        for container in [
            nested.clone(),
            DataType::List(Arc::new(Field::new("item", nested.clone(), true))),
            DataType::LargeList(Arc::new(Field::new("item", nested.clone(), true))),
            DataType::FixedSizeList(Arc::new(Field::new("item", nested.clone(), true)), 2),
        ] {
            let schema = Schema::new(vec![Field::new("root", container, true)]);
            assert!(validation::nested_field(&schema, &["root".into(), "value".into()]).is_none());
        }
    }
}

#[test]
fn compression_specs_visit_all_scopes_in_stable_order() {
    use open_arrow_sink_core::CompressionSpec::*;
    let options = CompressionOptions {
        global: Some(Snappy),
        columns: [("z".into(), Gzip), ("a".into(), Uncompressed)].into(),
        nested_columns: [
            (vec!["root".into(), "z".into()], Lz4Raw),
            (vec!["root".into(), "a".into()], Zstd { level: None }),
        ]
        .into(),
        ..Default::default()
    };
    assert_eq!(
        options.specs().collect::<Vec<_>>(),
        [Snappy, Uncompressed, Gzip, Zstd { level: None }, Lz4Raw]
    );
    assert_eq!(CompressionOptions::default().specs().count(), 0);
}

#[test]
fn zstd_levels_check_boundaries_and_every_scope() {
    use open_arrow_sink_core::{CompressionSpec, Error, ZSTD_COMPRESSION_LEVEL_RANGE};
    for level in [None, Some(-1), Some(0), Some(1), Some(22), Some(23)] {
        let spec = CompressionSpec::Zstd { level };
        for options in [
            CompressionOptions {
                global: Some(spec),
                ..Default::default()
            },
            CompressionOptions {
                columns: [("value".into(), spec)].into(),
                ..Default::default()
            },
            CompressionOptions {
                nested_columns: [(vec!["root".into(), "value".into()], spec)].into(),
                ..Default::default()
            },
        ] {
            for allow_zero in [false, true] {
                let result = validation::validate_zstd_levels(
                    &options,
                    ZSTD_COMPRESSION_LEVEL_RANGE,
                    allow_zero,
                );
                let valid = level
                    .is_none_or(|level| (1..=22).contains(&level) || (allow_zero && level == 0));
                if valid {
                    result.unwrap();
                } else {
                    assert!(matches!(result, Err(Error::InvalidConfig(_))));
                }
            }
        }
    }
    validation::validate_zstd_levels(
        &CompressionOptions {
            global: Some(CompressionSpec::Uncompressed),
            ..Default::default()
        },
        ZSTD_COMPRESSION_LEVEL_RANGE,
        false,
    )
    .unwrap();
}
