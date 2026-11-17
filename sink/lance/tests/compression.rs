use arrow_array::cast::AsArray;
use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray, StructArray};
use arrow_schema::DataType;
use more_asserts::assert_gt;
use open_arrow_sink_core::{
    CompressionOptions, CompressionSpec, Sink, SinkExt, StructuralEncoding, WriteMode,
};
use open_arrow_sink_lance::{LanceDatasetSink, Url};
use open_arrow_sink_layer::CompressionLayer;
use open_arrow_sink_test_support::{
    concat_sorted, seeded_bytes, sort_batch, unordered_options as options,
};
use std::sync::Arc;

fn batch(count: usize) -> RecordBatch {
    let text = Arc::new(StringArray::from_iter_values(
        (0..count).map(|i| format!("unique-{i:08}-{}", "payload ".repeat(100))),
    ));
    let nested = StructArray::from(
        RecordBatch::try_from_iter([("text", text.clone() as ArrayRef)]).unwrap(),
    );
    RecordBatch::try_from_iter([
        (
            "id",
            Arc::new(Int64Array::from_iter_values(0..count as i64)) as ArrayRef,
        ),
        ("nested", Arc::new(nested) as ArrayRef),
        ("raw", text as ArrayRef),
    ])
    .unwrap()
}

fn policy() -> CompressionOptions {
    CompressionOptions {
        global: Some(CompressionSpec::Zstd { level: Some(3) }),
        columns: [("raw".into(), CompressionSpec::Uncompressed)].into(),
        structural_encoding: Some(StructuralEncoding::Fullzip),
        column_encodings: [("raw".into(), StructuralEncoding::Miniblock)].into(),
        ..Default::default()
    }
}

#[rstest::rstest]
#[case::single_writer(false)]
#[case::parallel_writers(true)]
#[tokio::test]
async fn structural_encoding_changes_physical_pages_and_survives_rolling(#[case] parallel: bool) {
    use lance_file::reader::{FileReader, describe_encoding};
    use lance_io::utils::CachedFileSize;
    use lance_io::{
        object_store::ObjectStore,
        scheduler::{ScanScheduler, SchedulerConfig},
    };

    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("dataset");
    let url = Url::from_directory_path(&path).unwrap();
    let input = batch(10 * 1024);
    let compression = LanceDatasetSink::default().layer(CompressionLayer::new(policy()));
    let sink: Box<dyn Sink<Request = Url>> = if parallel {
        Box::new(
            compression.layer(open_arrow_sink_layer::UnorderedWriteLayer::new(
                std::num::NonZeroUsize::new(2).unwrap(),
            )),
        )
    } else {
        Box::new(compression)
    };
    let mut writer = sink
        .open(
            url.clone(),
            input.schema(),
            options()
                .with_storage_version("2.2".into())
                .with_target_file_size(1),
        )
        .await
        .unwrap();
    writer.write(input.clone()).await.unwrap();
    writer.write(input.clone()).await.unwrap();
    writer.commit().await.unwrap();

    let dataset = lance::Dataset::open(path.to_str().unwrap()).await.unwrap();
    let arrow_schema = arrow_schema::Schema::from(dataset.schema());
    let nested = arrow_schema.field_with_name("nested").unwrap();
    let arrow_schema::DataType::Struct(children) = nested.data_type() else {
        panic!("expected struct")
    };
    for field in [
        arrow_schema.field_with_name("id").unwrap(),
        nested,
        children[0].as_ref(),
    ] {
        assert_eq!(
            field.metadata()["lance-encoding:structural-encoding"],
            "fullzip"
        );
        assert_eq!(field.metadata()["lance-encoding:compression"], "zstd");
    }
    let raw = arrow_schema.field_with_name("raw").unwrap();
    assert_eq!(
        raw.metadata()["lance-encoding:structural-encoding"],
        "miniblock"
    );
    assert_eq!(raw.metadata()["lance-encoding:compression"], "none");
    let actual = dataset.scan().try_into_batch().await.unwrap();
    assert_eq!(actual.num_rows(), 2 * input.num_rows());
    let ids = actual
        .column_by_name("id")
        .unwrap()
        .as_primitive::<arrow_array::types::Int64Type>();
    let raw = actual.column_by_name("raw").unwrap().as_string::<i32>();
    let nested = actual.column_by_name("nested").unwrap().as_struct();
    let nested_text = nested.column(0).as_string::<i32>();
    let expected = input.column_by_name("raw").unwrap().as_string::<i32>();
    let mut occurrences = vec![0; input.num_rows()];
    for row in 0..actual.num_rows() {
        let id = usize::try_from(ids.value(row)).unwrap();
        assert_eq!(raw.value(row), expected.value(id));
        assert_eq!(nested_text.value(row), expected.value(id));
        occurrences[id] += 1;
    }
    assert_eq!(occurrences, vec![2; input.num_rows()]);

    let (store, base) = ObjectStore::from_uri(url.as_str()).await.unwrap();
    let scheduler = ScanScheduler::new(store, SchedulerConfig::default_for_testing());
    let fragments = dataset.get_fragments();
    assert!(fragments.len() >= 2);
    for fragment in fragments {
        for file in &fragment.metadata().files {
            let file = scheduler
                .open_file(
                    &base.clone().join("data").join(file.path.as_str()),
                    &CachedFileSize::unknown(),
                )
                .await
                .unwrap();
            let metadata = FileReader::read_all_metadata(&file).await.unwrap();
            let pages: Vec<_> = metadata
                .column_metadatas
                .iter()
                .flat_map(|column| &column.pages)
                .map(describe_encoding)
                .collect();
            assert!(
                pages.iter().any(|page| page.contains("FullZipLayout")),
                "{pages:?}"
            );
            assert!(
                pages.iter().any(|page| page.contains("MiniBlockLayout")),
                "{pages:?}"
            );
        }
    }
}

#[tokio::test]
async fn append_compares_effective_structural_encoding_schema() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("dataset");
    let url = Url::from_directory_path(&path).unwrap();
    let input = batch(1024);
    let sink = LanceDatasetSink::default();
    for mode in [WriteMode::Create, WriteMode::Append] {
        let mut writer = sink
            .open(
                url.clone(),
                input.schema(),
                options().with_compression(policy()).with_mode(mode),
            )
            .await
            .unwrap();
        writer.write(input.clone()).await.unwrap();
        writer.commit().await.unwrap();
    }
    let mut changed = policy();
    changed.structural_encoding = Some(StructuralEncoding::Miniblock);
    assert!(matches!(
        sink.open(
            url,
            input.schema(),
            options()
                .with_compression(changed)
                .with_mode(WriteMode::Append)
        )
        .await,
        Err(open_arrow_sink_core::Error::SchemaMismatch)
    ));
    let dataset = lance::Dataset::open(path.to_str().unwrap()).await.unwrap();
    assert_eq!(dataset.count_rows(None).await.unwrap(), 2048);
}

#[tokio::test]
async fn nested_override_preserves_sibling_policy_and_rows() {
    let input = batch(64);
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("dataset");
    let options = options().with_compression(CompressionOptions {
        global: Some(CompressionSpec::Zstd { level: Some(3) }),
        nested_columns: [(
            vec!["nested".into(), "text".into()],
            CompressionSpec::Uncompressed,
        )]
        .into(),
        ..Default::default()
    });
    let sink = LanceDatasetSink::default();
    let mut writer = sink
        .open(
            Url::from_directory_path(&path).unwrap(),
            input.schema(),
            options,
        )
        .await
        .unwrap();
    assert_eq!(writer.schema(), &input.schema());
    writer.write(input.clone()).await.unwrap();
    writer.commit().await.unwrap();
    let dataset = lance::Dataset::open(path.to_str().unwrap()).await.unwrap();
    let actual = dataset.scan().try_into_batch().await.unwrap();
    let schema = actual.schema();
    let arrow_schema::DataType::Struct(children) = schema.field(1).data_type() else {
        panic!("struct")
    };
    assert_eq!(children[0].metadata()["lance-encoding:compression"], "none");
    assert!(
        !children[0]
            .metadata()
            .contains_key("lance-encoding:compression-level")
    );
    assert_eq!(
        schema.field(2).metadata()["lance-encoding:compression"],
        "zstd"
    );
    assert_eq!(actual.num_rows(), input.num_rows());
    let nested = actual.column(1).as_struct();
    let expected = input.column(1).as_struct();
    assert_eq!(nested.column(0), expected.column(0));
}

async fn compressed_dataset(count: usize) -> (tempfile::TempDir, lance::Dataset, RecordBatch) {
    use std::collections::HashMap;

    use arrow_array::{FixedSizeBinaryArray, StringArray, StructArray};
    use arrow_schema::{DataType, Field, Schema};
    use open_arrow_sink_core::{CompressionOptions, CompressionSpec, SinkExt};
    use open_arrow_sink_layer::CompressionLayer;

    let text = StringArray::from_iter_values(
        (0..count).map(|id| format!("{id:08}:{}", "compressible payload ".repeat(100))),
    );
    let raw =
        FixedSizeBinaryArray::try_from_iter((0..count).map(|id| seeded_bytes(id as u64 + 1, 1024)))
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
    let sink = LanceDatasetSink::default().layer(CompressionLayer::new(CompressionOptions {
        global: Some(CompressionSpec::Zstd { level: Some(9) }),
        columns: [
            ("nested".into(), CompressionSpec::Zstd { level: Some(3) }),
            ("raw_payload".into(), CompressionSpec::Uncompressed),
        ]
        .into(),
        ..Default::default()
    }));
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("dataset");
    let mut writer = sink
        .open(
            Url::from_directory_path(std::path::absolute(&path).unwrap()).unwrap(),
            schema.clone(),
            options().with_target_file_size(1),
        )
        .await
        .unwrap();
    assert_eq!(writer.schema(), &schema);
    writer.write(expected.clone()).await.unwrap();
    writer.write(expected.clone()).await.unwrap();
    writer.commit().await.unwrap();

    let dataset = lance::Dataset::open(path.to_str().unwrap()).await.unwrap();
    (temp, dataset, expected)
}

#[tokio::test]
async fn compression_overrides_nested_and_column_metadata() {
    let (_temp, dataset, expected) = compressed_dataset(64).await;
    let schema = expected.schema();
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
}

#[tokio::test]
async fn compression_survives_rolling_and_round_trips() {
    let (_temp, dataset, expected) = compressed_dataset(10 * 1024).await;
    let actual = dataset.scan().try_into_batch().await.unwrap();
    assert_gt!(dataset.get_fragments().len(), 1);
    // Encoding metadata intentionally differs. Compare every value separately
    // from the metadata assertions above, retaining both copies of every row.
    fn values(batch: &RecordBatch) -> RecordBatch {
        RecordBatch::try_from_iter([
            ("id", batch.column(0).clone()),
            ("text", batch.column(1).as_struct().column(0).clone()),
            ("raw_payload", batch.column(2).clone()),
        ])
        .unwrap()
    }
    let actual_values = sort_batch(&values(&actual), &["id"]).unwrap();
    let expected_values = values(&expected);
    let expected_values = concat_sorted(
        &expected_values.schema(),
        &[expected_values.clone(), expected_values],
        &["id"],
    )
    .unwrap();
    assert_eq!(actual_values, expected_values);
}

#[tokio::test]
async fn invalid_zstd_levels_are_rejected_before_destination_io() {
    for level in [-1, 23] {
        let spec = CompressionSpec::Zstd { level: Some(level) };
        for compression in [
            CompressionOptions {
                global: Some(spec),
                ..Default::default()
            },
            CompressionOptions {
                columns: [("raw".into(), spec)].into(),
                ..Default::default()
            },
            CompressionOptions {
                nested_columns: [(vec!["nested".into(), "text".into()], spec)].into(),
                ..Default::default()
            },
        ] {
            let temp = tempfile::tempdir().unwrap();
            let path = temp.path().join("uncreated");
            let result = LanceDatasetSink::default()
                .open(
                    Url::from_directory_path(&path).unwrap(),
                    batch(1).schema(),
                    options().with_compression(compression),
                )
                .await;
            assert!(matches!(
                result,
                Err(open_arrow_sink_core::Error::InvalidConfig(_))
            ));
            assert!(!path.exists());
        }
    }
}
