use std::sync::Arc;

use arrow_array::{Array, ArrayRef, Int64Array, RecordBatch};
use lance::Dataset;
use open_arrow_sink_core::{Ordering, Sink, WriteOptions};
use open_arrow_sink_lance::LanceDatasetSink;

fn batch() -> RecordBatch {
    RecordBatch::try_from_iter([(
        "value",
        Arc::new(Int64Array::from(vec![Some(3), None, Some(1), Some(3)])) as ArrayRef,
    )])
    .unwrap()
}

fn options() -> WriteOptions {
    WriteOptions {
        ordering: Ordering::Unordered,
        ..WriteOptions::default()
    }
}

#[tokio::test]
async fn commit_round_trips_multiple_batches() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("dataset");
    let expected = batch();
    let mut writer = LanceDatasetSink
        .open(path.clone(), expected.schema(), options())
        .await
        .unwrap();
    assert_eq!(writer.schema(), &expected.schema());
    writer.write(expected.slice(0, 1)).await.unwrap();
    writer.write(expected.slice(1, 3)).await.unwrap();
    assert_eq!(writer.commit().await.unwrap().rows_written, 4);
    let dataset = Dataset::open(path.to_str().unwrap()).await.unwrap();
    let actual = dataset.scan().try_into_batch().await.unwrap();
    assert_eq!(actual.schema(), expected.schema());
    let values = actual
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let mut values: Vec<_> = (0..values.len())
        .map(|i| (!values.is_null(i)).then(|| values.value(i)))
        .collect();
    values.sort_unstable();
    assert_eq!(values, vec![None, Some(1), Some(3), Some(3)]);
}

#[tokio::test]
async fn empty_commit_produces_readable_dataset() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("dataset");
    let schema = batch().schema();
    let writer = LanceDatasetSink
        .open(path.clone(), schema.clone(), options())
        .await
        .unwrap();
    assert_eq!(writer.commit().await.unwrap().rows_written, 0);
    let dataset = Dataset::open(path.to_str().unwrap()).await.unwrap();
    assert_eq!(dataset.count_rows(None).await.unwrap(), 0);
    assert_eq!(
        dataset.scan().try_into_batch().await.unwrap().schema(),
        schema
    );
}

#[tokio::test]
async fn abort_removes_dataset_and_preserves_sibling_files() {
    let temp = tempfile::tempdir().unwrap();
    let sibling = temp.path().join("keep.txt");
    tokio::fs::write(&sibling, "keep").await.unwrap();
    let path = temp.path().join("dataset");
    let batch = batch();
    let mut writer = LanceDatasetSink
        .open(path.clone(), batch.schema(), options())
        .await
        .unwrap();
    writer.write(batch).await.unwrap();
    writer.abort().await.unwrap();
    assert!(!tokio::fs::try_exists(&path).await.unwrap());
    assert_eq!(tokio::fs::read_to_string(sibling).await.unwrap(), "keep");
}

#[rstest::rstest]
#[case::default_size(None, false)]
#[case::small_target(Some(1), true)]
#[tokio::test]
async fn target_file_size_rolls_files_without_losing_rows(
    #[case] target_file_size: Option<usize>,
    #[case] should_roll: bool,
) {
    // Exceed the SDK's default page buffer while staying below its row-count limit.
    let count = 10 * 1024;
    let payloads = arrow_array::FixedSizeBinaryArray::try_from_iter((0..count).map(|i| {
        let mut state = i as u64 + 1;
        let mut bytes = [0; 1024];
        for byte in &mut bytes {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            *byte = state as u8;
        }
        bytes
    }))
    .unwrap();
    let expected = RecordBatch::try_from_iter([
        (
            "id",
            Arc::new(Int64Array::from_iter_values(0..count as i64)) as ArrayRef,
        ),
        ("payload", Arc::new(payloads) as ArrayRef),
    ])
    .unwrap();
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("dataset");
    let mut writer = LanceDatasetSink
        .open(
            path.clone(),
            expected.schema(),
            WriteOptions {
                target_file_size,
                ..options()
            },
        )
        .await
        .unwrap();
    writer.write(expected.clone()).await.unwrap();
    writer.write(expected.clone()).await.unwrap();
    assert_eq!(
        writer.commit().await.unwrap().rows_written,
        2 * count as u64
    );
    let dataset = Dataset::open(path.to_str().unwrap()).await.unwrap();
    let fragments = dataset.get_fragments();
    if should_roll {
        more_asserts::assert_gt!(fragments.len(), 1);
    } else {
        assert_eq!(fragments.len(), 1);
    }
    let actual = dataset.scan().try_into_batch().await.unwrap();
    assert_eq!(actual.schema(), expected.schema());
    let ids = actual
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let payloads = actual
        .column(1)
        .as_any()
        .downcast_ref::<arrow_array::FixedSizeBinaryArray>()
        .unwrap();
    let expected_payloads = expected
        .column(1)
        .as_any()
        .downcast_ref::<arrow_array::FixedSizeBinaryArray>()
        .unwrap();
    let mut occurrences = vec![0; count];
    for i in 0..actual.num_rows() {
        let id = usize::try_from(ids.value(i)).unwrap();
        assert_eq!(payloads.value(i), expected_payloads.value(id));
        occurrences[id] += 1;
    }
    assert_eq!(occurrences, vec![2; count]);
}

#[tokio::test]
async fn compression_survives_rolling_and_round_trips() {
    use std::collections::HashMap;

    use arrow_array::{FixedSizeBinaryArray, StringArray, StructArray};
    use arrow_schema::{DataType, Field, Schema};
    use open_arrow_sink_core::{Compression, CompressionOptions, SinkExt};
    use open_arrow_sink_layer::CompressionLayer;

    let count = 10 * 1024;
    let text = StringArray::from_iter_values(
        (0..count).map(|id| format!("{id:08}:{}", "compressible payload ".repeat(100))),
    );
    let raw = FixedSizeBinaryArray::try_from_iter((0..count).map(|id| {
        let mut state = id as u64 + 1;
        let mut bytes = [0; 1024];
        for byte in &mut bytes {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            *byte = state as u8;
        }
        bytes
    }))
    .unwrap();
    let text_field = Arc::new(Field::new("text", DataType::Utf8, false).with_metadata(
        HashMap::from([
            ("custom".into(), "preserved".into()),
            ("lance-encoding:compression".into(), "none".into()),
            ("lance-encoding:compression-level".into(), "17".into()),
        ]),
    ));
    let nested = StructArray::new(vec![text_field].into(), vec![Arc::new(text.clone())], None);
    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from_iter_values(0..count as i64)),
        Arc::new(nested),
        Arc::new(raw.clone()),
    ];
    let schema = Arc::new(Schema::new_with_metadata(
        vec![
            Field::new("id", columns[0].data_type().clone(), false),
            Field::new("nested", columns[1].data_type().clone(), false),
            Field::new("raw_payload", columns[2].data_type().clone(), false),
        ],
        HashMap::from([("application".into(), "preserved".into())]),
    ));
    let expected = RecordBatch::try_new(schema.clone(), columns).unwrap();
    let sink = LanceDatasetSink.layer(CompressionLayer::new(CompressionOptions {
        global: Some(Compression::Zstd { level: Some(9) }),
        columns: [
            ("nested".into(), Compression::Zstd { level: Some(3) }),
            ("raw_payload".into(), Compression::Uncompressed),
        ]
        .into(),
    }));
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("dataset");
    let mut writer = sink
        .open(
            path.clone(),
            schema.clone(),
            WriteOptions {
                target_file_size: Some(1),
                ..options()
            },
        )
        .await
        .unwrap();
    assert_eq!(writer.schema(), &schema);
    writer.write(expected.clone()).await.unwrap();
    writer.write(expected.clone()).await.unwrap();
    assert_eq!(
        writer.commit().await.unwrap().rows_written,
        2 * count as u64
    );

    let dataset = Dataset::open(path.to_str().unwrap()).await.unwrap();
    assert!(dataset.get_fragments().len() > 1);
    let actual = dataset.scan().try_into_batch().await.unwrap();
    let stored_schema = actual.schema();
    assert_eq!(stored_schema.metadata(), schema.metadata());
    assert_eq!(
        stored_schema.field(0).metadata()["lance-encoding:compression"],
        "zstd"
    );
    assert_eq!(
        stored_schema.field(0).metadata()["lance-encoding:compression-level"],
        "9"
    );
    let DataType::Struct(children) = stored_schema.field(1).data_type() else {
        panic!("expected nested struct");
    };
    assert_eq!(children[0].metadata()["custom"], "preserved");
    assert_eq!(children[0].metadata()["lance-encoding:compression"], "zstd");
    assert_eq!(
        children[0].metadata()["lance-encoding:compression-level"],
        "3"
    );
    assert_eq!(
        stored_schema.field(2).metadata()["lance-encoding:compression"],
        "none"
    );
    assert!(
        !stored_schema
            .field(2)
            .metadata()
            .contains_key("lance-encoding:compression-level")
    );
    // The original schema and its existing metadata are still the caller's contract.
    let DataType::Struct(children) = schema.field(1).data_type() else {
        panic!("expected nested struct");
    };
    assert_eq!(children[0].metadata()["lance-encoding:compression"], "none");
    assert_eq!(
        children[0].metadata()["lance-encoding:compression-level"],
        "17"
    );

    let ids = actual
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let nested = actual
        .column(1)
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    let actual_text = nested
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let actual_raw = actual
        .column(2)
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    let mut occurrences = vec![0; count];
    for i in 0..actual.num_rows() {
        let id = usize::try_from(ids.value(i)).unwrap();
        assert_eq!(actual_text.value(i), text.value(id));
        assert_eq!(actual_raw.value(i), raw.value(id));
        occurrences[id] += 1;
    }
    assert_eq!(occurrences, vec![2; count]);
}

#[tokio::test]
async fn explicit_modes_create_append_and_overwrite_versions() {
    use open_arrow_sink_core::WriteMode;
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("dataset");
    for (mode, rows) in [
        (WriteMode::Create, 4),
        (WriteMode::Append, 8),
        (WriteMode::Overwrite, 4),
    ] {
        let mut writer = LanceDatasetSink
            .open(
                path.clone(),
                batch().schema(),
                WriteOptions {
                    mode: Some(mode),
                    ..options()
                },
            )
            .await
            .unwrap();
        writer.write(batch()).await.unwrap();
        assert_eq!(writer.commit().await.unwrap().rows_written, 4);
        assert_eq!(
            Dataset::open(path.to_str().unwrap())
                .await
                .unwrap()
                .count_rows(None)
                .await
                .unwrap(),
            rows
        );
    }
    assert!(
        LanceDatasetSink
            .open(
                path,
                batch().schema(),
                WriteOptions {
                    mode: Some(WriteMode::Create),
                    ..options()
                }
            )
            .await
            .is_err()
    );
}

#[tokio::test]
async fn abort_append_and_overwrite_preserves_committed_version() {
    use open_arrow_sink_core::WriteMode;
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("dataset");
    let mut writer = LanceDatasetSink
        .open(path.clone(), batch().schema(), options())
        .await
        .unwrap();
    writer.write(batch()).await.unwrap();
    writer.commit().await.unwrap();
    let version = Dataset::open(path.to_str().unwrap())
        .await
        .unwrap()
        .version()
        .version;
    for mode in [WriteMode::Append, WriteMode::Overwrite] {
        let mut writer = LanceDatasetSink
            .open(
                path.clone(),
                batch().schema(),
                WriteOptions {
                    mode: Some(mode),
                    ..options()
                },
            )
            .await
            .unwrap();
        writer.write(batch()).await.unwrap();
        writer.abort().await.unwrap();
        let dataset = Dataset::open(path.to_str().unwrap()).await.unwrap();
        assert_eq!(dataset.version().version, version);
        assert_eq!(dataset.count_rows(None).await.unwrap(), 4);
    }
}

#[tokio::test]
async fn append_requires_existing_compatible_dataset() {
    use open_arrow_sink_core::WriteMode;
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("dataset");
    let append = WriteOptions {
        mode: Some(WriteMode::Append),
        ..options()
    };
    assert!(
        LanceDatasetSink
            .open(path.clone(), batch().schema(), append.clone())
            .await
            .is_err()
    );
    let writer = LanceDatasetSink
        .open(path.clone(), batch().schema(), options())
        .await
        .unwrap();
    writer.commit().await.unwrap();
    let wrong =
        RecordBatch::try_from_iter([("other", Arc::new(Int64Array::from(vec![1])) as ArrayRef)])
            .unwrap();
    assert!(matches!(
        LanceDatasetSink.open(path, wrong.schema(), append).await,
        Err(open_arrow_sink_core::Error::SchemaMismatch)
    ));
}

#[tokio::test]
async fn empty_overwrite_creates_or_clears_dataset() {
    use open_arrow_sink_core::WriteMode;
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("dataset");
    let overwrite = WriteOptions {
        mode: Some(WriteMode::Overwrite),
        ..options()
    };
    LanceDatasetSink
        .open(path.clone(), batch().schema(), overwrite.clone())
        .await
        .unwrap()
        .commit()
        .await
        .unwrap();
    let mut writer = LanceDatasetSink
        .open(
            path.clone(),
            batch().schema(),
            WriteOptions {
                mode: Some(WriteMode::Append),
                ..options()
            },
        )
        .await
        .unwrap();
    writer.write(batch()).await.unwrap();
    writer.commit().await.unwrap();
    LanceDatasetSink
        .open(path.clone(), batch().schema(), overwrite)
        .await
        .unwrap()
        .commit()
        .await
        .unwrap();
    assert_eq!(
        Dataset::open(path.to_str().unwrap())
            .await
            .unwrap()
            .count_rows(None)
            .await
            .unwrap(),
        0
    );
}

#[tokio::test]
async fn append_reuses_compression_schema() {
    use open_arrow_sink_core::{Compression, CompressionOptions, WriteMode};
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("dataset");
    let compressed = WriteOptions {
        compression: CompressionOptions {
            global: Some(Compression::Zstd { level: Some(3) }),
            ..Default::default()
        },
        ..options()
    };
    for mode in [WriteMode::Create, WriteMode::Append] {
        let mut writer = LanceDatasetSink
            .open(
                path.clone(),
                batch().schema(),
                WriteOptions {
                    mode: Some(mode),
                    ..compressed.clone()
                },
            )
            .await
            .unwrap();
        writer.write(batch()).await.unwrap();
        writer.commit().await.unwrap();
    }
    assert_eq!(
        Dataset::open(path.to_str().unwrap())
            .await
            .unwrap()
            .count_rows(None)
            .await
            .unwrap(),
        8
    );
}

#[tokio::test]
async fn overwrite_can_replace_schema_without_changing_previous_version() {
    use open_arrow_sink_core::WriteMode;
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("dataset");
    let mut writer = LanceDatasetSink
        .open(path.clone(), batch().schema(), options())
        .await
        .unwrap();
    writer.write(batch()).await.unwrap();
    writer.commit().await.unwrap();
    let previous = Dataset::open(path.to_str().unwrap()).await.unwrap();
    let replacement =
        RecordBatch::try_from_iter([("other", Arc::new(Int64Array::from(vec![99])) as ArrayRef)])
            .unwrap();
    let mut writer = LanceDatasetSink
        .open(
            path.clone(),
            replacement.schema(),
            WriteOptions {
                mode: Some(WriteMode::Overwrite),
                ..options()
            },
        )
        .await
        .unwrap();
    writer.write(replacement.clone()).await.unwrap();
    writer.commit().await.unwrap();
    let dataset = Dataset::open(path.to_str().unwrap()).await.unwrap();
    assert_eq!(dataset.scan().try_into_batch().await.unwrap(), replacement);
    assert_eq!(previous.count_rows(None).await.unwrap(), 4);
}
