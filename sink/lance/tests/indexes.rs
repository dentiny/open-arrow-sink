use std::sync::Arc;

use arrow_array::{
    ArrayRef, FixedSizeListArray, Float32Array, Int64Array, RecordBatch, StringArray,
};
use arrow_schema::{DataType, Field};
use lance::{Dataset, index::DatasetIndexExt};
use lance_index::scalar::FullTextSearchQuery;
use open_arrow_sink_core::{IndexSpec as Spec, IndexType as Kind, Ordering, Sink, WriteOptions};
use open_arrow_sink_lance::{LanceDatasetSink, Url};

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

fn vectors(count: usize, dimension: i32) -> RecordBatch {
    let vector = FixedSizeListArray::new(
        Arc::new(Field::new("item", DataType::Float32, true)),
        dimension,
        Arc::new(Float32Array::from_iter_values(
            (0..count * dimension as usize).map(|i| (i as f32).sin()),
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

#[rstest::rstest]
#[case::small_dimension(2)]
#[case::prime_dimension(17)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn builds_all_index_kinds_and_queries_them(#[case] dimension: i32) {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("indexed");
    let batch = vectors(256, dimension);
    let specs = vec![
        Spec::new("id", Kind::Scalar),
        Spec::new("text", Kind::Text),
        Spec::new("category", Kind::BloomFilter),
        Spec::new("vector", Kind::Vector),
    ];
    let sink = LanceDatasetSink::default();
    let mut writer = sink
        .open(
            Url::from_directory_path(&path).unwrap(),
            batch.schema(),
            indexed_options(specs.clone()),
        )
        .await
        .unwrap();
    writer.write(batch.slice(0, 100)).await.unwrap();
    writer.write(batch.slice(100, 156)).await.unwrap();
    assert_eq!(writer.commit().await.unwrap().rows_written, 256);
    let dataset = Dataset::open(path.to_str().unwrap()).await.unwrap();
    let descriptions = dataset.describe_indices(None).await.unwrap();
    for (spec, expected) in specs
        .iter()
        .zip(["BTree", "Inverted", "BloomFilter", "IVF_PQ"])
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
    let query = Float32Array::from_iter_values((0..dimension).map(|i| (i as f32).sin()));
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

#[tokio::test]
async fn small_vector_dataset_remains_queryable_when_training_is_deferred() {
    let temp = tempfile::tempdir().unwrap();
    let url = Url::from_directory_path(temp.path().join("small")).unwrap();
    // An 8-bit PQ codebook needs 256 training vectors. Lance can retain an
    // untrained index declaration and search uncovered rows directly.
    let batch = vectors(255, 2);
    let sink = LanceDatasetSink::default();
    let mut writer = sink
        .open(
            url.clone(),
            batch.schema(),
            indexed_options(vec![Spec::new("vector", Kind::Vector)]),
        )
        .await
        .unwrap();
    writer.write(batch).await.unwrap();
    assert_eq!(writer.commit().await.unwrap().rows_written, 255);
    let dataset = sink.dataset_builder(url).unwrap().load().await.unwrap();
    let descriptions = dataset.describe_indices(None).await.unwrap();
    assert_eq!(descriptions.len(), 1);
    assert_eq!(descriptions[0].index_type().to_lowercase(), "ivf_pq");
    assert_eq!(dataset.count_rows(None).await.unwrap(), 255);
    let found = dataset
        .scan()
        .nearest("vector", &Float32Array::from(vec![0.0, 1.0_f32.sin()]), 1)
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
