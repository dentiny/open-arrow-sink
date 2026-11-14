use std::{num::NonZeroUsize, sync::Arc};

use arrow_array::{
    Array, ArrayRef, FixedSizeListArray, Float32Array, Int64Array, RecordBatch, StringArray,
};
use arrow_schema::{DataType, Field, Schema};
use lance::{Dataset, index::DatasetIndexExt};
use lance_index::scalar::FullTextSearchQuery;
use open_arrow_sink_core::{Error, Ordering, Sink, SinkExt, WriteOptions};
use open_arrow_sink_lance::{
    IndexBuildState, IndexCommitError, LanceDatasetSink, LanceIndexKind as Kind, LanceIndexLayer,
    LanceIndexSpec as Spec, LancePartitionLayer, PartitionCommitError, PartitionCommitState,
    PartitionKey, retry_indexes,
};

fn options() -> WriteOptions {
    WriteOptions {
        ordering: Ordering::Unordered,
        ..Default::default()
    }
}
fn partition_layer() -> LancePartitionLayer {
    LancePartitionLayer::new(vec!["key".into()], NonZeroUsize::new(8).unwrap())
}
fn input(keys: Vec<Option<&str>>) -> RecordBatch {
    let count = keys.len();
    RecordBatch::try_from_iter([
        ("key", Arc::new(StringArray::from(keys)) as ArrayRef),
        (
            "id",
            Arc::new(Int64Array::from_iter_values(0..count as i64)) as ArrayRef,
        ),
    ])
    .unwrap()
}
fn vectors(count: usize) -> RecordBatch {
    let vector = FixedSizeListArray::new(
        Arc::new(Field::new("item", DataType::Float32, true)),
        2,
        Arc::new(Float32Array::from_iter_values(
            (0..count * 2).map(|i| (i as f32).sin()),
        )),
        None,
    );
    RecordBatch::try_from_iter([
        (
            "id",
            Arc::new(Int64Array::from_iter_values(0..count as i64)) as ArrayRef,
        ),
        (
            "text",
            Arc::new(StringArray::from_iter_values((0..count).map(|i| {
                if i % 2 == 0 {
                    "hello world"
                } else {
                    "goodbye world"
                }
            }))) as ArrayRef,
        ),
        (
            "category",
            Arc::new(StringArray::from_iter_values(
                (0..count).map(|i| if i % 2 == 0 { "even" } else { "odd" }),
            )) as ArrayRef,
        ),
        ("vector", Arc::new(vector) as ArrayRef),
    ])
    .unwrap()
}

#[tokio::test]
async fn builds_all_index_kinds_and_queries_them() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("indexed");
    let batch = vectors(256);
    let specs = vec![
        Spec::new("id", Kind::Scalar),
        Spec::new("text", Kind::Text),
        Spec::new("category", Kind::BloomFilter),
        Spec::new("vector", Kind::Vector),
    ];
    let sink = LanceDatasetSink.layer(LanceIndexLayer::new(specs.clone()));
    let mut writer = sink
        .open(path.clone(), batch.schema(), options())
        .await
        .unwrap();
    writer.write(batch.slice(0, 100)).await.unwrap();
    writer.write(batch.slice(100, 156)).await.unwrap();
    assert_eq!(writer.commit().await.unwrap().rows_written, 256);
    let dataset = Dataset::open(path.to_str().unwrap()).await.unwrap();
    let descriptions = dataset.describe_indices(None).await.unwrap();
    for (spec, expected) in specs
        .iter()
        .zip(["BTree", "Inverted", "BloomFilter", "IVF_FLAT"])
    {
        let description = descriptions
            .iter()
            .find(|d| d.name() == spec.name())
            .unwrap();
        assert_eq!(
            description.index_type().to_lowercase(),
            expected.to_lowercase()
        );
    }
    let metadata = dataset.load_indices().await.unwrap();
    assert_eq!(metadata.len(), 4);
    for index in metadata.iter() {
        let bitmap = index.fragment_bitmap.as_ref().unwrap();
        assert!(
            dataset
                .get_fragments()
                .iter()
                .all(|f| bitmap.contains(f.id() as u32))
        );
    }
    assert_eq!(
        dataset
            .scan()
            .filter("id = 42")
            .unwrap()
            .try_into_batch()
            .await
            .unwrap()
            .num_rows(),
        1
    );
    assert_eq!(
        dataset
            .scan()
            .filter("category = 'even'")
            .unwrap()
            .try_into_batch()
            .await
            .unwrap()
            .num_rows(),
        128
    );
    assert_eq!(
        dataset
            .scan()
            .full_text_search(FullTextSearchQuery::new("hello".into()))
            .unwrap()
            .try_into_batch()
            .await
            .unwrap()
            .num_rows(),
        128
    );
    let query = Float32Array::from(vec![0.0, 1.0_f32.sin()]);
    let found = dataset
        .scan()
        .nearest("vector", &query, 1)
        .unwrap()
        .try_into_batch()
        .await
        .unwrap();
    assert_eq!(
        found
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        0
    );
}

#[rstest::rstest]
#[case::missing(vec![Spec::new("missing", Kind::Scalar)])]
#[case::duplicate(vec![Spec::new("id", Kind::Scalar), Spec::new("id", Kind::BloomFilter)])]
#[case::wrong_type(vec![Spec::new("id", Kind::Vector)])]
#[tokio::test]
async fn invalid_index_configuration_has_no_side_effects(#[case] specs: Vec<Spec>) {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("partitions");
    let batch = input(vec![Some("a")]);
    let sink = LanceDatasetSink
        .layer(LanceIndexLayer::new(specs))
        .layer(partition_layer());
    assert!(
        sink.open(path.clone(), batch.schema(), options())
            .await
            .is_err()
    );
    assert!(!path.exists());
}

#[tokio::test]
async fn routes_null_empty_and_path_values_without_loss_and_indexes_each_dataset() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("partitions");
    let keys = vec![
        None,
        Some(""),
        Some("../outside"),
        Some("a/b"),
        Some("a/b"),
        None,
    ];
    let batch = input(keys.clone());
    let spec = Spec::new("id", Kind::Scalar);
    let sink = LanceDatasetSink
        .layer(LanceIndexLayer::new(vec![spec.clone()]))
        .layer(partition_layer());
    let mut writer = sink
        .open(path.clone(), batch.schema(), options())
        .await
        .unwrap();
    writer.write(batch.slice(0, 3)).await.unwrap();
    writer.write(batch.slice(3, 3)).await.unwrap();
    assert_eq!(writer.commit().await.unwrap().rows_written, 6);
    let mut found = Vec::new();
    for value in [None, Some(""), Some("../outside"), Some("a/b")] {
        let key = PartitionKey {
            values: vec![value.map(str::to_owned)],
        };
        let dataset = Dataset::open(path.join(key.directory_name()).to_str().unwrap())
            .await
            .unwrap();
        assert_eq!(dataset.load_indices().await.unwrap()[0].name, spec.name());
        let actual = dataset.scan().try_into_batch().await.unwrap();
        let actual_keys = actual
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let ids = actual
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for i in 0..actual.num_rows() {
            assert_eq!(actual_keys.is_null(i), value.is_none());
            if let Some(value) = value {
                assert_eq!(actual_keys.value(i), value);
            }
            assert_eq!(keys[ids.value(i) as usize], value);
            found.push(ids.value(i));
        }
    }
    found.sort_unstable();
    assert_eq!(found, vec![0, 1, 2, 3, 4, 5]);
    assert!(!temp.path().join("outside").exists());
}

#[tokio::test]
async fn partition_limit_is_terminal_and_commit_does_not_publish_partial_input() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("partitions");
    let batch = input(vec![Some("a"), Some("b")]);
    let sink = LanceDatasetSink.layer(LancePartitionLayer::new(
        vec!["key".into()],
        NonZeroUsize::new(1).unwrap(),
    ));
    let mut writer = sink
        .open(path.clone(), batch.schema(), options())
        .await
        .unwrap();
    writer.write(batch.slice(0, 1)).await.unwrap();
    assert!(writer.write(batch.slice(1, 1)).await.is_err());
    assert!(writer.commit().await.is_err());
    assert!(!path.exists());
}

#[tokio::test]
async fn batch_budget_and_schema_errors_are_terminal() {
    let temp = tempfile::tempdir().unwrap();
    let batch = input(vec![Some("a")]);
    let sink = LanceDatasetSink
        .layer(partition_layer().with_max_batch_bytes(NonZeroUsize::new(1).unwrap()));
    let mut writer = sink
        .open(temp.path().join("budget"), batch.schema(), options())
        .await
        .unwrap();
    assert!(writer.write(batch.clone()).await.is_err());
    assert!(writer.commit().await.is_err());
    let sink = LanceDatasetSink.layer(partition_layer());
    let mut writer = sink
        .open(temp.path().join("schema"), batch.schema(), options())
        .await
        .unwrap();
    let bad = RecordBatch::try_from_iter([("id", batch.column(1).clone())]).unwrap();
    assert!(matches!(
        writer.write(bad).await,
        Err(Error::SchemaMismatch)
    ));
    assert!(writer.write(batch).await.is_err());
    writer.abort().await.unwrap();
}

#[tokio::test]
async fn abort_honors_cleanup_policy_and_empty_input_creates_no_datasets() {
    let temp = tempfile::tempdir().unwrap();
    let batch = input(vec![Some("a")]);
    let sink = LanceDatasetSink.layer(partition_layer());
    for cleanup in [false, true] {
        let path = temp.path().join(format!("abort-{cleanup}"));
        let mut writer = sink
            .open(
                path.clone(),
                batch.schema(),
                WriteOptions {
                    cleanup_on_error: cleanup,
                    ..options()
                },
            )
            .await
            .unwrap();
        writer.write(batch.clone()).await.unwrap();
        writer.abort().await.unwrap();
        assert_eq!(path.exists(), !cleanup);
    }
    let path = temp.path().join("empty");
    let mut writer = sink
        .open(path.clone(), batch.schema(), options())
        .await
        .unwrap();
    writer.write(batch.slice(0, 0)).await.unwrap();
    assert_eq!(writer.commit().await.unwrap().rows_written, 0);
    assert_eq!(std::fs::read_dir(path).unwrap().count(), 0);
}

#[tokio::test]
async fn rejects_existing_paths_ordering_and_invalid_partition_columns() {
    let temp = tempfile::tempdir().unwrap();
    let batch = input(vec![Some("a")]);
    let sink = LanceDatasetSink.layer(partition_layer());
    assert!(
        sink.open(temp.path().to_path_buf(), batch.schema(), options())
            .await
            .is_err()
    );
    let path = temp.path().join("ordered");
    assert!(
        sink.open(path.clone(), batch.schema(), WriteOptions::default())
            .await
            .is_err()
    );
    assert!(!path.exists());
    for columns in [
        vec![],
        vec!["key".into(), "key".into()],
        vec!["missing".into()],
    ] {
        let sink = LanceDatasetSink.layer(LancePartitionLayer::new(
            columns,
            NonZeroUsize::new(2).unwrap(),
        ));
        assert!(
            sink.open(path.clone(), batch.schema(), options())
                .await
                .is_err()
        );
        assert!(!path.exists());
    }
}

fn failed_vector_input() -> RecordBatch {
    let field = Arc::new(Field::new("item", DataType::Float32, true));
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("vector", DataType::FixedSizeList(field.clone(), 2), true),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(FixedSizeListArray::new_null(field, 2, 2)),
        ],
    )
    .unwrap()
}

#[tokio::test]
async fn index_failure_preserves_data_and_retry_does_not_rebuild_successful_indexes() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("failed-index");
    let batch = failed_vector_input();
    let sink = LanceDatasetSink.layer(LanceIndexLayer::new(vec![
        Spec::new("id", Kind::Scalar),
        Spec::new("vector", Kind::Vector),
    ]));
    let mut writer = sink
        .open(path.clone(), batch.schema(), options())
        .await
        .unwrap();
    writer.write(batch).await.unwrap();
    let error = writer.commit().await.unwrap_err();
    let Error::Backend(error) = error else {
        panic!("expected index error")
    };
    let error = error.downcast::<IndexCommitError>().unwrap();
    let dataset = Dataset::open(path.to_str().unwrap()).await.unwrap();
    assert_eq!(dataset.count_rows(None).await.unwrap(), 2);
    let indexes = dataset.load_indices().await.unwrap();
    assert_eq!(indexes.len(), 1);
    assert!(matches!(
        error.report.indexes[0].state,
        IndexBuildState::Complete { .. }
    ));
    assert!(matches!(
        error.report.indexes[1].state,
        IndexBuildState::Failed { .. }
    ));
    assert!(error.report.data_version.unwrap() < error.report.latest_version.unwrap());
    let retried = retry_indexes(error.report.clone()).await.unwrap_err();
    assert_eq!(retried.report.latest_version, error.report.latest_version);
    let dataset = Dataset::open(path.to_str().unwrap()).await.unwrap();
    assert_eq!(dataset.count_rows(None).await.unwrap(), 2);
    assert_eq!(
        dataset.load_indices().await.unwrap()[0].uuid,
        indexes[0].uuid
    );
    // A stale report must fail before creating any more indexes.
    let mut stale = retried.report;
    stale.latest_version = stale.data_version;
    assert!(
        retry_indexes(stale)
            .await
            .unwrap_err()
            .message
            .contains("version changed")
    );
}

#[tokio::test]
async fn partition_errors_preserve_post_commit_index_reports() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("partitions");
    let batch = failed_vector_input();
    let sink = LanceDatasetSink
        .layer(LanceIndexLayer::new(vec![Spec::new(
            "vector",
            Kind::Vector,
        )]))
        .layer(LancePartitionLayer::new(
            vec!["id".into()],
            NonZeroUsize::new(2).unwrap(),
        ));
    let mut writer = sink.open(path, batch.schema(), options()).await.unwrap();
    writer.write(batch).await.unwrap();
    let Error::Backend(error) = writer.commit().await.unwrap_err() else {
        panic!("expected partition error")
    };
    let report = error.downcast::<PartitionCommitError>().unwrap();
    assert_eq!(report.partitions.len(), 2);
    for partition in &report.partitions {
        let PartitionCommitState::Failed {
            index_report: Some(index_report),
            ..
        } = &partition.state
        else {
            panic!("missing index report")
        };
        assert!(index_report.data_version.is_some());
        let dataset = Dataset::open(partition.path.to_str().unwrap())
            .await
            .unwrap();
        assert_eq!(dataset.count_rows(None).await.unwrap(), 1);
    }
}

#[tokio::test]
async fn retry_can_finish_pending_indexes_without_writing_rows_or_replacing_existing_indexes() {
    use open_arrow_sink_lance::{IndexBuildReport, IndexBuildResult};
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("retry");
    let batch = vectors(64);
    let mut writer = LanceDatasetSink
        .open(path.clone(), batch.schema(), options())
        .await
        .unwrap();
    writer.write(batch).await.unwrap();
    writer.commit().await.unwrap();
    let version = Dataset::open(path.to_str().unwrap())
        .await
        .unwrap()
        .version_id();
    let report = IndexBuildReport {
        path: path.clone(),
        rows_written: 64,
        data_version: Some(version),
        latest_version: Some(version),
        indexes: vec![IndexBuildResult {
            spec: Spec::new("id", Kind::Scalar),
            state: IndexBuildState::Pending,
        }],
    };
    let report = retry_indexes(report).await.unwrap();
    let IndexBuildState::Complete { uuid } = &report.indexes[0].state else {
        panic!("index not completed")
    };
    let report = retry_indexes(report.clone()).await.unwrap();
    let dataset = Dataset::open(path.to_str().unwrap()).await.unwrap();
    assert_eq!(dataset.count_rows(None).await.unwrap(), 64);
    assert_eq!(dataset.version_id(), report.latest_version.unwrap());
    assert_eq!(
        dataset.load_indices().await.unwrap()[0].uuid.to_string(),
        *uuid
    );
    // Even after reconciliation to the current version, a pending name collision
    // must not replace an existing index.
    let mut collision = report;
    collision.indexes[0].state = IndexBuildState::Pending;
    assert!(retry_indexes(collision).await.is_err());
    assert_eq!(
        Dataset::open(path.to_str().unwrap())
            .await
            .unwrap()
            .load_indices()
            .await
            .unwrap()[0]
            .uuid
            .to_string(),
        *uuid
    );
}

#[tokio::test]
async fn multiple_partition_columns_preserve_nulls_types_and_column_order() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("multi");
    let batch = RecordBatch::try_from_iter([
        (
            "label",
            Arc::new(StringArray::from(vec![Some(""), None, Some(""), None])) as ArrayRef,
        ),
        (
            "flag",
            Arc::new(arrow_array::BooleanArray::from(vec![
                true, true, false, true,
            ])) as ArrayRef,
        ),
        (
            "id",
            Arc::new(Int64Array::from(vec![0, 1, 2, 3])) as ArrayRef,
        ),
    ])
    .unwrap();
    let sink = LanceDatasetSink.layer(LancePartitionLayer::new(
        vec!["flag".into(), "label".into()],
        NonZeroUsize::new(3).unwrap(),
    ));
    let mut writer = sink
        .open(path.clone(), batch.schema(), options())
        .await
        .unwrap();
    writer.write(batch).await.unwrap();
    assert_eq!(writer.commit().await.unwrap().rows_written, 4);
    for (key, count) in [
        (vec![Some("true".to_owned()), Some("".to_owned())], 1),
        (vec![Some("true".to_owned()), None], 2),
        (vec![Some("false".to_owned()), Some("".to_owned())], 1),
    ] {
        let dataset = Dataset::open(
            path.join(PartitionKey { values: key }.directory_name())
                .to_str()
                .unwrap(),
        )
        .await
        .unwrap();
        assert_eq!(dataset.count_rows(None).await.unwrap(), count);
    }
}

#[tokio::test]
async fn rejects_long_keys_and_unsupported_key_types_without_child_writes() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("long");
    let text = "x".repeat(101);
    let batch = input(vec![Some(&text)]);
    let sink = LanceDatasetSink.layer(partition_layer());
    let mut writer = sink
        .open(path.clone(), batch.schema(), options())
        .await
        .unwrap();
    assert!(writer.write(batch).await.is_err());
    assert_eq!(std::fs::read_dir(&path).unwrap().count(), 0);
    writer.abort().await.unwrap();
    let batch =
        RecordBatch::try_from_iter([("key", Arc::new(Float32Array::from(vec![1.0])) as ArrayRef)])
            .unwrap();
    assert!(
        sink.open(path.clone(), batch.schema(), options())
            .await
            .is_err()
    );
    assert!(!path.exists());
}
