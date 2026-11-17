use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use futures::TryStreamExt;
use open_arrow_sink_core::{Sink, SinkExt, WriteMode, WriteOptions};
use open_arrow_sink_parquet::{ParquetDirectorySink, Url};
use open_arrow_sink_test_support::unordered_options as options;
use parquet::arrow::ParquetRecordBatchStreamBuilder;
use std::{path::Path, sync::Arc};

fn batch() -> RecordBatch {
    RecordBatch::try_from_iter([
        (
            "id",
            Arc::new(Int64Array::from(vec![Some(3), None, Some(1)])) as ArrayRef,
        ),
        (
            "text",
            Arc::new(StringArray::from(vec![Some("雪"), Some(""), None])) as ArrayRef,
        ),
    ])
    .unwrap()
}

#[rstest::rstest]
#[case::default(None)]
#[case::large_target(Some(256 * 1024 * 1024))]
#[tokio::test]
async fn commit_round_trips_multiple_batches(#[case] target_file_size: Option<usize>) {
    let temp = tempfile::tempdir().unwrap();
    let directory = temp.path().join("output");
    let expected = batch();
    let mut write_options = options();
    write_options.target_file_size = target_file_size;
    let mut writer = ParquetDirectorySink::default()
        .open(
            Url::from_directory_path(&directory).unwrap(),
            expected.schema(),
            write_options,
        )
        .await
        .unwrap();
    assert_eq!(writer.schema(), &expected.schema());
    writer.write(expected.slice(0, 1)).await.unwrap();
    writer.write(expected.slice(1, 2)).await.unwrap();
    let summary = writer.commit().await.unwrap();
    assert_eq!(summary.rows_committed, 3);
    assert_eq!(summary.rows_skipped, 0);
    assert_eq!(summary.rows_failed_blobs, 0);

    let file = tokio::fs::File::open(directory.join("part-0-00000.parquet"))
        .await
        .unwrap();
    let builder = ParquetRecordBatchStreamBuilder::new(file).await.unwrap();
    // Small input batches share a row group, including nulls and strings.
    assert_eq!(builder.metadata().num_row_groups(), 1);
    let mut reader = builder.build().unwrap();
    let mut offset = 0;
    while let Some(actual) = reader.try_next().await.unwrap() {
        assert_eq!(actual, expected.slice(offset, actual.num_rows()));
        offset += actual.num_rows();
    }
    assert_eq!(offset, expected.num_rows());
}

#[tokio::test]
async fn write_all_round_trips_the_input_stream() {
    let temp = tempfile::tempdir().unwrap();
    let directory = temp.path().join("output");
    let expected = batch();
    let summary = ParquetDirectorySink::default()
        .write_all(
            Url::from_directory_path(&directory).unwrap(),
            expected.schema(),
            options(),
            futures::stream::iter([
                Ok::<_, open_arrow_sink_core::Error>(expected.slice(0, 1)),
                Ok(expected.slice(1, 2)),
            ]),
        )
        .await
        .unwrap();
    assert_eq!(summary.rows_committed, 3);

    let file = tokio::fs::File::open(directory.join("part-0-00000.parquet"))
        .await
        .unwrap();
    let batches: Vec<_> = ParquetRecordBatchStreamBuilder::new(file)
        .await
        .unwrap()
        .build()
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    assert_eq!(batches, [expected]);
}

struct ConformanceFixture {
    destination: open_arrow_sink_test_support::TempDirDestination,
    sink: ParquetDirectorySink,
}

#[open_arrow_sink_core::async_trait]
impl open_arrow_sink_test_support::conformance::SinkFixture for ConformanceFixture {
    type Sink = ParquetDirectorySink;
    async fn new() -> Self {
        Self {
            destination: open_arrow_sink_test_support::TempDirDestination::new("output"),
            sink: ParquetDirectorySink::default(),
        }
    }
    fn sink(&self) -> &Self::Sink {
        &self.sink
    }
    fn request(&self) -> Url {
        self.destination.url()
    }
    fn batch(&self) -> RecordBatch {
        batch()
    }
    fn options(&self) -> WriteOptions {
        options()
    }
    async fn read(&self) -> Vec<RecordBatch> {
        let batches = read_dir(self.destination.path()).await;
        assert!(
            !batches.is_empty(),
            "even an empty commit must contain readable Parquet"
        );
        for actual in &batches {
            assert_eq!(actual.schema(), batch().schema());
        }
        batches
    }
    async fn assert_rolled(&self) {
        let directory = self.request();
        let storage = open_arrow_sink_storage::Storage::from_url(&directory).unwrap();
        assert!(
            open_arrow_sink_test_support::sorted_list(&storage, &storage.path(&directory).unwrap())
                .await
                .len()
                > 1
        );
    }
    async fn create_sibling(&self) {
        self.destination.create_sibling().await;
    }
    async fn assert_sibling(&self) {
        self.destination.assert_sibling().await;
    }
    async fn assert_aborted(&self) {
        // Lazy writers may abort before creating the destination. Once data
        // exists, abort must retain it (also covered by the preservation test).
        if tokio::fs::try_exists(self.destination.path())
            .await
            .unwrap()
        {
            assert!(
                tokio::fs::read_dir(self.destination.path())
                    .await
                    .unwrap()
                    .next_entry()
                    .await
                    .unwrap()
                    .is_some()
            );
        }
    }
}

open_arrow_sink_test_support::conformance_tests!(ConformanceFixture);

#[tokio::test]
async fn abort_preserves_output_and_sibling_files() {
    let temp = tempfile::tempdir().unwrap();
    let sibling = temp.path().join("keep.txt");
    tokio::fs::write(&sibling, "keep").await.unwrap();
    let directory = temp.path().join("output");
    let batch = batch();
    let mut writer = ParquetDirectorySink::default()
        .open(
            Url::from_directory_path(&directory).unwrap(),
            batch.schema(),
            options().with_target_file_size(1),
        )
        .await
        .unwrap();
    writer.write(batch.clone()).await.unwrap();
    writer.write(batch).await.unwrap();
    writer.abort().await.unwrap();
    assert!(tokio::fs::try_exists(&directory).await.unwrap());
    assert_eq!(tokio::fs::read_to_string(sibling).await.unwrap(), "keep");
}

#[tokio::test]
async fn target_file_size_rolls_files_without_losing_rows() {
    let expected = RecordBatch::try_from_iter([
        (
            "id",
            Arc::new(Int64Array::from_iter_values(0..9)) as ArrayRef,
        ),
        (
            "text",
            Arc::new(StringArray::from(vec!["雪"; 9])) as ArrayRef,
        ),
    ])
    .unwrap();
    let temp = tempfile::tempdir().unwrap();
    let directory = temp.path().join("output");
    let mut writer = ParquetDirectorySink::default()
        .open(
            Url::from_directory_path(&directory).unwrap(),
            expected.schema(),
            options().with_target_file_size(1),
        )
        .await
        .unwrap();
    writer.write(expected.slice(0, 0)).await.unwrap();
    for offset in [0, 3, 6] {
        writer.write(expected.slice(offset, 3)).await.unwrap();
    }
    writer.write(expected.slice(0, 0)).await.unwrap();
    writer.commit().await.unwrap();
    let url = Url::from_directory_path(&directory).unwrap();
    let storage = open_arrow_sink_storage::Storage::from_url(&url).unwrap();
    assert_eq!(
        storage
            .list(&storage.path(&url).unwrap())
            .await
            .unwrap()
            .len(),
        3
    );
    let batches = read_dir(&directory).await;
    let mut offset = 0;
    for actual in batches {
        assert_eq!(actual, expected.slice(offset, actual.num_rows()));
        offset += actual.num_rows();
    }
    assert_eq!(offset, 9);
}

async fn read_dir(directory: &Path) -> Vec<RecordBatch> {
    let directory = Url::from_directory_path(directory).unwrap();
    let storage = open_arrow_sink_storage::Storage::from_url(&directory).unwrap();
    open_arrow_sink_test_support::read_parquet_dir(&storage, &storage.path(&directory).unwrap())
        .await
}

async fn row_count(directory: &Path) -> usize {
    read_dir(directory)
        .await
        .iter()
        .map(RecordBatch::num_rows)
        .sum()
}

#[tokio::test]
async fn append_adds_rows() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("output");
    let batch = batch();
    let mut writer = ParquetDirectorySink::default()
        .open(
            Url::from_directory_path(&path).unwrap(),
            batch.schema(),
            options(),
        )
        .await
        .unwrap();
    writer.write(batch.slice(0, 1)).await.unwrap();
    writer.commit().await.unwrap();
    let mut writer = ParquetDirectorySink::default()
        .open(
            Url::from_directory_path(&path).unwrap(),
            batch.schema(),
            options().with_mode(WriteMode::Append),
        )
        .await
        .unwrap();
    writer.write(batch).await.unwrap();
    writer.commit().await.unwrap();
    assert_eq!(row_count(&path).await, 4);
}

#[tokio::test]
async fn idle_writers_do_not_create_parquet_files() {
    use open_arrow_sink_core::ParallelSink;
    use std::num::NonZeroUsize;

    let temp = tempfile::tempdir().unwrap();
    let directory = temp.path().join("output");
    let input = batch();
    let mut session = ParquetDirectorySink::default()
        .open_writers(
            Url::from_directory_path(&directory).unwrap(),
            input.schema(),
            options(),
            NonZeroUsize::new(8).unwrap(),
        )
        .await
        .unwrap();
    assert!(!directory.exists());
    session.writers[0].write(input.slice(0, 0)).await.unwrap();
    session.writers[1].write(input.clone()).await.unwrap();
    session.commit().await.unwrap();
    let files = std::fs::read_dir(&directory)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(files.len(), 1);
    let file = tokio::fs::File::open(files[0].path()).await.unwrap();
    let mut reader = ParquetRecordBatchStreamBuilder::new(file)
        .await
        .unwrap()
        .build()
        .unwrap();
    assert_eq!(reader.try_next().await.unwrap().unwrap(), input);
    assert!(reader.try_next().await.unwrap().is_none());
}
