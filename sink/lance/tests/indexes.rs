use std::sync::Arc;

use arrow_array::{
    ArrayRef, FixedSizeListArray, Float32Array, Int64Array, RecordBatch, StringArray,
};
use arrow_schema::{DataType, Field, Schema};
use lance::{Dataset, index::DatasetIndexExt};
use lance_index::scalar::FullTextSearchQuery;
use open_arrow_sink_core::{
    Error, IndexCommitError, IndexSpec as Spec, IndexType as Kind, Ordering, Sink, WriteOptions,
};
use open_arrow_sink_lance::LanceDatasetSink;

fn options() -> WriteOptions {
    WriteOptions {
        ordering: Ordering::Unordered,
        ..Default::default()
    }
}

fn indexed_options(indexes: Vec<Spec>) -> WriteOptions {
    WriteOptions {
        indexes,
        ..options()
    }
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
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
    let sink = LanceDatasetSink;
    let mut writer = sink
        .open(path.clone(), batch.schema(), indexed_options(specs.clone()))
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
        let field_id = u32::try_from(dataset.schema().field(&spec.column).unwrap().id).unwrap();
        let description = descriptions
            .iter()
            .find(|d| d.field_ids() == [field_id])
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
    let path = temp.path().join("invalid-index");
    let batch = vectors(1);
    let sink = LanceDatasetSink;
    assert!(
        sink.open(path.clone(), batch.schema(), indexed_options(specs.clone()))
            .await
            .is_err()
    );
    assert!(!path.exists());
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_index_failure_waits_for_other_indexes_and_preserves_data() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("failed-index");
    let batch = failed_vector_input();
    let sink = LanceDatasetSink;
    let indexes = vec![
        Spec::new("vector", Kind::Vector),
        Spec::new("id", Kind::Scalar),
    ];
    let mut writer = sink
        .open(path.clone(), batch.schema(), indexed_options(indexes))
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
    assert!(error.message.contains("Vector"));
    assert!(error.message.contains("vector"));
}

#[tokio::test]
async fn empty_index_list_preserves_empty_dataset_commit() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("no-indexes");
    let sink = LanceDatasetSink;
    let writer = sink
        .open(path.clone(), vectors(0).schema(), options())
        .await
        .unwrap();
    assert_eq!(writer.commit().await.unwrap().rows_written, 0);
    let dataset = Dataset::open(path.to_str().unwrap()).await.unwrap();
    assert_eq!(dataset.count_rows(None).await.unwrap(), 0);
    assert!(dataset.load_indices().await.unwrap().is_empty());
}

#[tokio::test]
async fn indexed_sink_preserves_ordering_existing_destination_and_abort_contracts() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("indexed");
    let batch = vectors(4);
    let sink = LanceDatasetSink;
    assert!(
        sink.open(
            path.clone(),
            batch.schema(),
            WriteOptions {
                ordering: Ordering::PreserveInput,
                ..indexed_options(vec![Spec::new("id", Kind::Scalar)])
            }
        )
        .await
        .is_err()
    );
    assert!(!path.exists());
    let mut writer = sink
        .open(
            path.clone(),
            batch.schema(),
            indexed_options(vec![Spec::new("id", Kind::Scalar)]),
        )
        .await
        .unwrap();
    writer.write(batch.clone()).await.unwrap();
    assert!(Dataset::open(path.to_str().unwrap()).await.is_err());
    assert!(
        sink.open(
            path.clone(),
            batch.schema(),
            indexed_options(vec![Spec::new("id", Kind::Scalar)])
        )
        .await
        .is_err()
    );
    writer.abort().await.unwrap();
    assert!(!path.exists());
}
