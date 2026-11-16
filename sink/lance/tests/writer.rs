use arrow_array::{ArrayRef, Int64Array, RecordBatch};
use lance::Dataset;
use more_asserts::assert_gt;
use open_arrow_sink_core::{Sink, WriteMode, WriteOptions};
use open_arrow_sink_lance::{LanceDatasetSink, Url};
use open_arrow_sink_test_support::conformance::{Case, assert_case};
use open_arrow_sink_test_support::{seeded_bytes, unordered_options as options};
use std::sync::Arc;

fn batch() -> RecordBatch {
    open_arrow_sink_test_support::int64_batch("value", vec![Some(3), None, Some(1), Some(3)])
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
        vec![dataset.scan().try_into_batch().await.unwrap()]
    }
    fn rolling_batch(&self) -> RecordBatch {
        // Exceed the SDK's default page buffer while staying below its row-count limit.
        let count = 10 * 1024;
        let payloads = arrow_array::FixedSizeBinaryArray::try_from_iter(
            (0..count).map(|id| seeded_bytes(id as u64 + 1, 1024)),
        )
        .unwrap();
        RecordBatch::try_from_iter([
            (
                "id",
                Arc::new(Int64Array::from_iter_values(0..count as i64)) as ArrayRef,
            ),
            ("payload", Arc::new(payloads) as ArrayRef),
        ])
        .unwrap()
    }
    async fn assert_rolled(&self) {
        let dataset = Dataset::open(self.path.to_str().unwrap()).await.unwrap();
        assert_gt!(dataset.get_fragments().len(), 1);
    }
    async fn create_sibling(&self) {
        tokio::fs::write(self.path.parent().unwrap().join("keep.txt"), "keep")
            .await
            .unwrap();
    }
    async fn assert_sibling(&self) {
        assert_eq!(
            tokio::fs::read_to_string(self.path.parent().unwrap().join("keep.txt"))
                .await
                .unwrap(),
            "keep"
        );
    }
    async fn assert_aborted(&self) {
        assert!(Dataset::open(self.path.to_str().unwrap()).await.is_err());
    }
}

#[rstest::rstest]
#[case::round_trip(Case::RoundTrip)]
#[case::rolling(Case::Rolling)]
#[case::unsupported_modes(Case::UnsupportedModes)]
#[case::empty_commit(Case::EmptyCommit)]
#[case::abort(Case::Abort)]
#[case::schema_mismatch(Case::SchemaMismatch)]
#[case::abort_before_write(Case::AbortBeforeWrite)]
#[case::poisoned_commit(Case::PoisonedCommit)]
#[tokio::test]
async fn shared_sink_contracts(#[case] case: Case) {
    assert_case::<ConformanceFixture>(case).await;
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
    assert_eq!(dataset.version().version, 2);
}

#[rstest::rstest]
#[case::empty(vec![])]
#[case::empty_versions(vec!["_versions/"])]
#[case::retained_files(vec!["data/uncommitted.lance", "notes.txt"])]
#[tokio::test]
async fn overwrite_directory_without_dataset(#[case] paths: Vec<&str>) {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("dataset");
    tokio::fs::create_dir(&path).await.unwrap();
    for relative in &paths {
        let file = path.join(relative);
        if relative.ends_with('/') {
            tokio::fs::create_dir_all(file).await.unwrap();
        } else {
            tokio::fs::create_dir_all(file.parent().unwrap())
                .await
                .unwrap();
            tokio::fs::write(file, b"keep").await.unwrap();
        }
    }
    let url = Url::from_directory_path(&path).unwrap();
    // Recovery is specific to Overwrite; neither Create nor Append is relaxed.
    for mode in [WriteMode::Create, WriteMode::Append] {
        assert!(
            LanceDatasetSink::default()
                .open(url.clone(), batch().schema(), options().with_mode(mode))
                .await
                .is_err()
        );
    }
    let mut writer = LanceDatasetSink::default()
        .open(
            url,
            batch().schema(),
            options().with_mode(WriteMode::Overwrite),
        )
        .await
        .unwrap();
    writer.write(batch()).await.unwrap();
    writer.commit().await.unwrap();
    let dataset = Dataset::open(path.to_str().unwrap()).await.unwrap();
    assert_eq!(dataset.version().version, 1);
    assert_eq!(dataset.scan().try_into_batch().await.unwrap(), batch());
    for relative in paths.into_iter().filter(|path| !path.ends_with('/')) {
        assert_eq!(tokio::fs::read(path.join(relative)).await.unwrap(), b"keep");
    }
}

#[rstest::rstest]
#[case::exact_file("dataset")]
#[case::corrupt_manifest("dataset/_versions/1.manifest")]
#[tokio::test]
async fn overwrite_rejects_invalid_destination(#[case] relative: &str) {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("dataset");
    let file = temp.path().join(relative);
    tokio::fs::create_dir_all(file.parent().unwrap())
        .await
        .unwrap();
    tokio::fs::write(&file, b"keep").await.unwrap();
    assert!(
        LanceDatasetSink::default()
            .open(
                Url::from_directory_path(&path).unwrap(),
                batch().schema(),
                options().with_mode(WriteMode::Overwrite),
            )
            .await
            .is_err()
    );
    assert_eq!(tokio::fs::read(file).await.unwrap(), b"keep");
}
