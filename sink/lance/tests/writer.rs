use std::sync::Arc;

use arrow_array::{ArrayRef, Int64Array, RecordBatch};
use lance::Dataset;
use more_asserts::assert_gt;
use open_arrow_sink_core::{Sink, WriteMode, WriteOptions};
use open_arrow_sink_lance::{LanceDatasetSink, Url};

fn batch() -> RecordBatch {
    open_arrow_sink_test_support::int64_batch("value", vec![Some(3), None, Some(1), Some(3)])
}

use open_arrow_sink_test_support::{
    concat_sorted, seeded_bytes, sort_batch, unordered_options as options,
};

async fn read_sorted(dataset: &Dataset, key: &str) -> RecordBatch {
    sort_batch(&dataset.scan().try_into_batch().await.unwrap(), &[key]).unwrap()
}

struct ConformanceFixture {
    _temp: tempfile::TempDir,
    path: std::path::PathBuf,
    sink: LanceDatasetSink,
}

#[open_arrow_sink_core::async_trait]
impl open_arrow_sink_test_support::conformance::SinkFixture for ConformanceFixture {
    type Sink = LanceDatasetSink;
    async fn new() -> Self {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("dataset");
        Self {
            _temp: temp,
            path,
            sink: LanceDatasetSink::default(),
        }
    }
    fn sink(&self) -> &Self::Sink {
        &self.sink
    }
    fn request(&self) -> Url {
        Url::from_directory_path(&self.path).unwrap()
    }
    fn batch(&self) -> RecordBatch {
        batch()
    }
    fn options(&self) -> WriteOptions {
        options()
    }
    async fn read(&self) -> Vec<RecordBatch> {
        let dataset = Dataset::open(self.path.to_str().unwrap()).await.unwrap();
        vec![read_sorted(&dataset, "value").await]
    }
    async fn assert_aborted(&self) {
        assert!(Dataset::open(self.path.to_str().unwrap()).await.is_err());
    }
}

#[tokio::test]
async fn shared_sink_contracts() {
    open_arrow_sink_test_support::conformance::assert_conforms::<ConformanceFixture>().await;
}

#[tokio::test]
async fn abort_does_not_publish_or_touch_sibling_files() {
    let temp = tempfile::tempdir().unwrap();
    let sibling = temp.path().join("keep.txt");
    tokio::fs::write(&sibling, "keep").await.unwrap();
    let path = temp.path().join("dataset");
    let batch = batch();
    let mut writer = LanceDatasetSink::default()
        .open(
            Url::from_directory_path(&path).unwrap(),
            batch.schema(),
            options(),
        )
        .await
        .unwrap();
    writer.write(batch).await.unwrap();
    writer.abort().await.unwrap();
    assert!(Dataset::open(path.to_str().unwrap()).await.is_err());
    assert_eq!(tokio::fs::read_to_string(sibling).await.unwrap(), "keep");
}

#[tokio::test]
async fn target_file_size_rolls_files_without_losing_rows() {
    // Exceed the SDK's default page buffer while staying below its row-count limit.
    let count = 10 * 1024;
    let payloads = arrow_array::FixedSizeBinaryArray::try_from_iter(
        (0..count).map(|id| seeded_bytes(id as u64 + 1, 1024)),
    )
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
    let mut writer = LanceDatasetSink::default()
        .open(
            Url::from_directory_path(std::path::absolute(&path).unwrap()).unwrap(),
            expected.schema(),
            options().with_target_file_size(1),
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
    assert_gt!(fragments.len(), 1);
    let actual = read_sorted(&dataset, "id").await;
    let expected =
        concat_sorted(&expected.schema(), &[expected.clone(), expected], &["id"]).unwrap();
    assert_eq!(actual, expected);
}

#[rstest::rstest]
#[case::append(WriteMode::Append, 5)]
#[case::overwrite(WriteMode::Overwrite, 4)]
#[tokio::test]
async fn write_mode(#[case] mode: WriteMode, #[case] expected_rows: usize) {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("dataset");
    let batch = batch();
    let mut writer = LanceDatasetSink::default()
        .open(
            Url::from_directory_path(&path).unwrap(),
            batch.schema(),
            options(),
        )
        .await
        .unwrap();
    writer.write(batch.slice(0, 1)).await.unwrap();
    writer.commit().await.unwrap();
    let mut writer = LanceDatasetSink::default()
        .open(
            Url::from_directory_path(&path).unwrap(),
            batch.schema(),
            options().with_mode(mode),
        )
        .await
        .unwrap();
    writer.write(batch).await.unwrap();
    writer.commit().await.unwrap();
    let dataset = Dataset::open(path.to_str().unwrap()).await.unwrap();
    assert_eq!(dataset.count_rows(None).await.unwrap(), expected_rows);
}
