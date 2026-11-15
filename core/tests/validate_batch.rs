use std::{collections::HashMap, sync::Arc};

use arrow_array::RecordBatch;
use arrow_schema::{DataType, Field, Schema};
use open_arrow_sink_core::{Error, validation::validate_batch};

fn schema() -> Schema {
    Schema::new_with_metadata(
        vec![
            Field::new("id", DataType::Int64, false)
                .with_metadata(HashMap::from([("description".into(), "identifier".into())])),
            Field::new(
                "payload",
                DataType::Struct(vec![Field::new("value", DataType::Utf8, true)].into()),
                true,
            ),
        ],
        HashMap::from([("source".into(), "benchmark".into())]),
    )
}

#[test]
fn accepts_shared_and_independently_allocated_equal_schemas() {
    let shared = Arc::new(schema());
    let batch = RecordBatch::new_empty(shared.clone());
    validate_batch(&shared, &batch).unwrap();
    let independent = schema();
    assert!(!std::ptr::eq(&independent, shared.as_ref()));
    validate_batch(&independent, &batch).unwrap();
}

#[test]
fn rejects_schema_differences_including_nested_fields_and_metadata() {
    let expected = schema();
    let fields = expected.fields().to_vec();
    let metadata = expected.metadata().clone();
    let mut variants = vec![
        Schema::new_with_metadata(fields.clone(), HashMap::new()),
        Schema::new_with_metadata(vec![fields[1].clone(), fields[0].clone()], metadata.clone()),
    ];
    for field in [
        fields[0].as_ref().clone().with_nullable(true),
        fields[0].as_ref().clone().with_name("other"),
        fields[0].as_ref().clone().with_data_type(DataType::Int32),
        fields[0].as_ref().clone().with_metadata(HashMap::new()),
    ] {
        variants.push(Schema::new_with_metadata(
            vec![Arc::new(field), fields[1].clone()],
            metadata.clone(),
        ));
    }
    variants.push(Schema::new_with_metadata(
        vec![
            fields[0].clone(),
            Arc::new(Field::new(
                "payload",
                DataType::Struct(vec![Field::new("value", DataType::Utf8, false)].into()),
                true,
            )),
        ],
        metadata,
    ));
    for different in variants {
        let batch = RecordBatch::new_empty(Arc::new(different));
        assert!(matches!(
            validate_batch(&expected, &batch),
            Err(Error::SchemaMismatch)
        ));
    }
}
