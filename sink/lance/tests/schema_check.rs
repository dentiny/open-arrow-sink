use arrow_array::{Array, ArrayRef, BinaryArray, BinaryViewArray, LargeBinaryArray, RecordBatch};
use arrow_schema::{DataType, Field, Schema, UnionFields, UnionMode};
use open_arrow_sink_core::{
    BLOB_METADATA_KEY, CompressionSpec, Error, IndexSpec, IndexType, Layer, Ordering, ParallelSink,
    SchemaIssueKind as Kind, Sink, WriteOptions,
};
use open_arrow_sink_lance::{LanceDatasetSink, Url};
use std::{num::NonZeroUsize, sync::Arc};

fn options() -> WriteOptions {
    WriteOptions::default().with_ordering(Ordering::Unordered)
}
fn blob(name: &str, ty: DataType) -> Field {
    Field::new(name, ty, true).with_metadata([(BLOB_METADATA_KEY.into(), "true".into())].into())
}

#[test]
fn reports_all_names_and_types_with_literal_nested_paths() {
    let schema = Schema::new(vec![
        Field::new("", DataType::Int64, true),
        Field::new("a.b", DataType::Int64, true),
        Field::new("_rowid", DataType::Int64, true),
        Field::new(
            "outer",
            DataType::Struct(
                vec![
                    Field::new("same", DataType::Int64, true),
                    Field::new("same", DataType::Int64, true),
                    Field::new("_rowaddr", DataType::Int64, true),
                    Field::new("bad.name", DataType::Int64, true),
                    Field::new(
                        "union",
                        DataType::Union(
                            UnionFields::try_new(
                                [0],
                                [Field::new("member", DataType::Int64, true)],
                            )
                            .unwrap(),
                            UnionMode::Sparse,
                        ),
                        true,
                    ),
                    Field::new("variant", DataType::Binary, true).with_metadata(
                        [(
                            "ARROW:extension:name".into(),
                            "arrow.parquet.variant".into(),
                        )]
                        .into(),
                    ),
                ]
                .into(),
            ),
            true,
        ),
    ]);
    let issues = LanceDatasetSink::check_schema_only(&schema);
    for (path, kind) in [
        (vec![""], Kind::InvalidName),
        (vec!["a.b"], Kind::InvalidName),
        (vec!["_rowid"], Kind::ReservedName),
        (vec!["outer", "same"], Kind::Duplicate),
        (vec!["outer", "_rowaddr"], Kind::ReservedName),
        (vec!["outer", "bad.name"], Kind::InvalidName),
        (vec!["outer", "union"], Kind::UnsupportedType),
        (vec!["outer", "variant"], Kind::UnsupportedType),
    ] {
        assert!(
            issues
                .iter()
                .any(|issue| issue.path == path && issue.kind == kind),
            "{path:?}: {issues:?}"
        );
    }
}

#[test]
fn combines_schema_blob_index_and_encoding_failures() {
    let schema = Schema::new(vec![
        blob("bytes", DataType::Int64),
        Field::new("id", DataType::Int64, true),
    ]);
    let mut options = options();
    options.indexes = vec![
        IndexSpec::new("id", IndexType::Vector),
        IndexSpec::new("missing", IndexType::Text),
    ];
    options
        .compression
        .columns
        .insert("absent".into(), CompressionSpec::Snappy);
    options
        .compression
        .columns
        .insert("id".into(), CompressionSpec::Zstd { level: Some(99) });
    options.compression.column_encodings.insert(
        "unknown".into(),
        open_arrow_sink_core::StructuralEncoding::Fullzip,
    );
    options.storage_version = Some("0.1".into());
    options.target_file_size = Some(0);
    let issues = LanceDatasetSink::default().check_schema(&schema, &options);
    for (name, kind) in [
        ("bytes", Kind::BlobType),
        ("id", Kind::IndexType),
        ("missing", Kind::MissingColumn),
        ("absent", Kind::MissingColumn),
        ("unknown", Kind::MissingColumn),
        ("id", Kind::InvalidOption),
    ] {
        assert!(
            issues
                .iter()
                .any(|issue| issue.path == [name] && issue.kind == kind),
            "{issues:?}"
        );
    }
    assert!(
        issues
            .iter()
            .any(|issue| issue.message.contains("storage version")),
        "{issues:?}"
    );
    assert!(
        issues
            .iter()
            .any(|issue| issue.message.contains("target_file_size")),
        "{issues:?}"
    );
}

#[test]
fn candidates_match_preflight_and_blob_indexes_are_rejected() {
    for ty in [
        DataType::Utf8,
        DataType::LargeUtf8,
        DataType::Utf8View,
        DataType::Binary,
        DataType::LargeBinary,
        DataType::BinaryView,
    ] {
        assert!(LanceDatasetSink::blob_source_eligible(&ty));
        let schema = Schema::new(vec![blob("payload", ty)]);
        assert!(
            LanceDatasetSink::default()
                .check_schema(&schema, &options())
                .is_empty()
        );
    }
    assert!(!LanceDatasetSink::blob_source_eligible(&DataType::Int64));
    assert!(!LanceDatasetSink::index_eligible(
        IndexType::Text,
        &DataType::Int64
    ));
    for (ty, eligible) in [
        (DataType::Float16, true),
        (DataType::Float32, true),
        (DataType::Float64, true),
        (DataType::Int8, false),
    ] {
        let vector = DataType::FixedSizeList(Arc::new(Field::new("item", ty, true)), 3);
        assert_eq!(
            LanceDatasetSink::index_eligible(IndexType::Vector, &vector),
            eligible
        );
    }
    let schema = Schema::new(vec![blob("url", DataType::Utf8)]);
    let issues = LanceDatasetSink::default().check_schema(
        &schema,
        &options().with_indexes(vec![IndexSpec::new("url", IndexType::Text)]),
    );
    assert!(issues.iter().any(|issue| issue.kind == Kind::IndexType));
}

#[tokio::test]
async fn open_and_parallel_open_reject_before_constructing_storage() {
    let sink = LanceDatasetSink::default();
    let schema = Arc::new(Schema::new(vec![Field::new(
        "_rowid",
        DataType::Int64,
        false,
    )]));
    // This URL cannot produce a storage backend. Schema errors must win first.
    let url = Url::parse("unsupported-storage://bucket/dataset").unwrap();
    assert!(!sink.check_schema(&schema, &options()).is_empty());
    let result = sink.open(url.clone(), schema.clone(), options()).await;
    assert!(
        matches!(result, Err(Error::InvalidInput(message)) if message.contains("not a valid Lance column name"))
    );
    let result = sink
        .open_writers(url, schema, options(), NonZeroUsize::MIN)
        .await;
    assert!(
        matches!(result, Err(Error::InvalidInput(message)) if message.contains("not a valid Lance column name"))
    );
}

#[test]
fn options_layer_preserves_backend_checks_after_overrides() {
    let sink =
        open_arrow_sink_layer::OptionsLayer::new(|options| options.ordering = Ordering::Unordered)
            .layer(LanceDatasetSink::default());
    let schema = Schema::new(vec![Field::new("_rowid", DataType::Int64, false)]);
    let issues = sink.check_schema(&schema, &WriteOptions::default());
    assert!(issues.iter().any(|issue| issue.kind == Kind::ReservedName));
    assert!(
        !issues
            .iter()
            .any(|issue| issue.message.contains("preserving input row order"))
    );
}

#[tokio::test]
async fn binary_blob_candidates_write_and_preserve_nulls() {
    let arrays: Vec<ArrayRef> = vec![
        Arc::new(BinaryArray::from(vec![Some(b"hello".as_slice()), None])),
        Arc::new(BinaryViewArray::from(vec![Some(b"hello".as_slice()), None])),
        Arc::new(LargeBinaryArray::from(vec![
            Some(b"hello".as_slice()),
            None,
        ])),
    ];
    for (i, array) in arrays.into_iter().enumerate() {
        let schema = Arc::new(Schema::new(vec![blob(
            "payload",
            array.data_type().clone(),
        )]));
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join(format!("dataset{i}"));
        let sink = LanceDatasetSink::default();
        assert!(sink.check_schema(&schema, &options()).is_empty());
        let mut writer = sink
            .open(
                Url::from_directory_path(&path).unwrap(),
                schema.clone(),
                options(),
            )
            .await
            .unwrap();
        writer
            .write(RecordBatch::try_new(schema, vec![array]).unwrap())
            .await
            .unwrap();
        writer.commit().await.unwrap();
        let dataset = lance::Dataset::open(path.to_str().unwrap()).await.unwrap();
        let batch = dataset
            .scan()
            .blob_handling(lance::datatypes::BlobHandling::AllBinary)
            .try_into_batch()
            .await
            .unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.column(0).null_count(), 1);
        let values = batch
            .column(0)
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .unwrap();
        let actual = values
            .iter()
            .map(|value| value.map(<[u8]>::to_vec))
            .collect::<Vec<_>>();
        assert!(actual.contains(&Some(b"hello".to_vec())));
        assert!(actual.contains(&None));
    }
}

#[test]
fn download_layer_collects_all_bad_selections_and_backend_errors() {
    let sink = open_arrow_sink_layer::DownloadBlobLayer::new(
        [
            "missing1".to_string(),
            "missing2".into(),
            "number".into(),
            "url".into(),
        ]
        .into(),
    )
    .layer(LanceDatasetSink::default());
    let schema = Schema::new(vec![
        Field::new("number", DataType::Int64, true),
        Field::new("url", DataType::Utf8, true),
        Field::new("_rowoffset", DataType::Int64, true),
    ]);
    let issues = sink.check_schema(
        &schema,
        &options().with_indexes(vec![IndexSpec::new("url", IndexType::Text)]),
    );
    for (name, kind) in [
        ("missing1", Kind::MissingColumn),
        ("missing2", Kind::MissingColumn),
        ("number", Kind::BlobType),
        ("url", Kind::IndexType),
        ("_rowoffset", Kind::ReservedName),
    ] {
        assert!(
            issues
                .iter()
                .any(|issue| issue.path == [name] && issue.kind == kind),
            "{issues:?}"
        );
    }
}

#[test]
fn checks_nested_lists_and_rejects_invalid_vector_dimensions() {
    let schema = Schema::new(vec![
        Field::new(
            "records",
            DataType::List(Arc::new(Field::new(
                "item",
                DataType::Struct(
                    vec![
                        Field::new("_row_created_at_version", DataType::Int64, true),
                        Field::new("duplicate", DataType::Utf8, true),
                        Field::new("duplicate", DataType::Utf8, true),
                        blob("selected", DataType::Utf8),
                    ]
                    .into(),
                ),
                true,
            ))),
            true,
        ),
        Field::new(
            "vector",
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 0),
            true,
        ),
    ]);
    let issues = LanceDatasetSink::check_schema_only(&schema);
    for (path, kind) in [
        (
            vec!["records", "item", "_row_created_at_version"],
            Kind::ReservedName,
        ),
        (vec!["records", "item", "duplicate"], Kind::Duplicate),
        (vec!["records", "item", "selected"], Kind::BlobType),
        (vec!["vector"], Kind::UnsupportedType),
    ] {
        assert!(
            issues
                .iter()
                .any(|issue| issue.path == path && issue.kind == kind),
            "{issues:?}"
        );
    }
}
