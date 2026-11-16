use arrow_array::RecordBatch;
use open_arrow_sink_core::{Sink, SinkExt, UploadConfig, WriteMode, WriteOptions};
use open_arrow_sink_layer::UnorderedWriteLayer;
use open_arrow_sink_parquet::{ParquetDirectorySink, Storage};
use std::num::NonZeroUsize;

fn storage() -> Storage {
    open_arrow_sink_test_support::memory_storage("memory:///warehouse/")
}
fn batch() -> RecordBatch {
    open_arrow_sink_test_support::int64_batch("value", vec![Some(1), Some(2), Some(3)])
}
fn options() -> WriteOptions {
    open_arrow_sink_test_support::unordered_options()
        .with_target_file_size(1)
        .with_upload(
            UploadConfig::default()
                .with_part_size(NonZeroUsize::new(16).unwrap())
                .with_concurrency(NonZeroUsize::new(2).unwrap()),
        )
}

#[tokio::test]
async fn shared_storage_abort_preserves_output() {
    let storage = storage();
    let directory = storage.url("output/").unwrap();
    let mut writer = ParquetDirectorySink::new(storage.clone())
        .open(directory.clone(), batch().schema(), options())
        .await
        .unwrap();
    writer.write(batch()).await.unwrap();
    writer.write(batch()).await.unwrap();
    writer.abort().await.unwrap();
    assert!(
        !storage
            .list(&storage.resolve(&directory).unwrap())
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn rolling_collision_is_preserved() {
    let storage = storage();
    let sink = ParquetDirectorySink::new(storage.clone());
    let directory = storage.url("output/").unwrap();
    let mut writer = sink
        .open(directory.clone(), batch().schema(), options())
        .await
        .unwrap();
    writer.write(batch()).await.unwrap();
    let collision = directory.join("part-0-00001.parquet").unwrap();
    storage
        .create(&storage.resolve(&collision).unwrap(), "keep".into())
        .await
        .unwrap();
    assert!(writer.write(batch()).await.is_err());
    let completed = storage
        .resolve(&directory.join("part-0-00000.parquet").unwrap())
        .unwrap();
    let bytes = storage.read(&completed).await.unwrap();
    let path = storage.resolve(&directory).unwrap();
    let mut paths = storage.list(&path).await.unwrap();
    paths.sort_by(|a, b| a.as_str().cmp(b.as_str()));
    assert!(writer.commit().await.is_err());
    let mut retained = storage.list(&path).await.unwrap();
    retained.sort_by(|a, b| a.as_str().cmp(b.as_str()));
    assert_eq!(retained, paths);
    assert_eq!(storage.read(&completed).await.unwrap(), bytes);
    assert_eq!(
        storage
            .read(&storage.resolve(&collision).unwrap())
            .await
            .unwrap(),
        "keep"
    );
}

async fn rows(storage: &Storage, directory: &open_arrow_sink_parquet::Url) -> usize {
    open_arrow_sink_test_support::read_parquet_dir(storage, directory)
        .await
        .iter()
        .map(RecordBatch::num_rows)
        .sum()
}

#[rstest::rstest]
#[case::memory(false)]
#[case::filesystem(true)]
#[tokio::test]
async fn overwrite_is_unsupported_on_every_backend(#[case] filesystem: bool) {
    let temp = tempfile::tempdir().unwrap();
    let (sink, storage, directory) = if filesystem {
        let directory =
            open_arrow_sink_parquet::Url::from_directory_path(temp.path().join("output")).unwrap();
        let storage = Storage::from_url(&directory).unwrap();
        (ParquetDirectorySink::default(), storage, directory)
    } else {
        let storage = storage();
        let directory = storage.url("output/").unwrap();
        (
            ParquetDirectorySink::new(storage.clone()),
            storage,
            directory,
        )
    };
    let mut original = sink
        .open(directory.clone(), batch().schema(), options())
        .await
        .unwrap();
    original.write(batch()).await.unwrap();
    original.commit().await.unwrap();
    assert!(
        !sink
            .capability()
            .supported_modes
            .contains(&WriteMode::Overwrite)
    );
    let result = sink
        .layer(UnorderedWriteLayer::new(NonZeroUsize::new(3).unwrap()))
        .open(
            directory.clone(),
            batch().schema(),
            options().with_mode(WriteMode::Overwrite),
        )
        .await;
    assert!(matches!(
        result,
        Err(open_arrow_sink_core::Error::Unsupported(_))
    ));
    assert_eq!(rows(&storage, &directory).await, 3);
}

#[tokio::test]
async fn session_observer_counts_parquet_output() {
    let storage = storage();
    let metrics = open_arrow_sink_core::IoMetrics::default();
    let mut writer = ParquetDirectorySink::new(storage.clone())
        .open(
            storage.url("measured/").unwrap(),
            batch().schema(),
            options().with_io_metrics(metrics.clone()),
        )
        .await
        .unwrap();
    writer.write(batch()).await.unwrap();
    writer.commit().await.unwrap();
    let directory = storage.resolve(&storage.url("measured/").unwrap()).unwrap();
    let mut output_bytes = 0;
    for file in storage.list(&directory).await.unwrap() {
        output_bytes += storage.stat(&file).await.unwrap().unwrap().content_length();
    }
    assert!(output_bytes > 0);
    assert_eq!(metrics.snapshot().bytes_written, output_bytes);
}

#[rstest::rstest]
#[case::memory(false)]
#[case::filesystem(true)]
#[tokio::test]
async fn empty_parallel_create_writes_one_file_and_append_adds_none(#[case] filesystem: bool) {
    use open_arrow_sink_core::ParallelSink;

    let temp = tempfile::tempdir().unwrap();
    let (storage, directory) = if filesystem {
        let directory =
            open_arrow_sink_parquet::Url::from_directory_path(temp.path().join("output")).unwrap();
        (Storage::from_url(&directory).unwrap(), directory)
    } else {
        let storage = storage();
        let directory = storage.url("output/").unwrap();
        (storage, directory)
    };
    let sink = ParquetDirectorySink::new(storage.clone());
    let path = storage.resolve(&directory).unwrap();
    for mode in [WriteMode::Create, WriteMode::Append] {
        let before = storage.list(&path).await.unwrap();
        let mut session = sink
            .open_writers(
                directory.clone(),
                batch().schema(),
                options().with_mode(mode),
                NonZeroUsize::new(8).unwrap(),
            )
            .await
            .unwrap();
        for writer in &mut session.writers {
            writer.write(batch().slice(0, 0)).await.unwrap();
        }
        let mut outputs = Vec::new();
        for writer in &mut session.writers {
            outputs.push(writer.finish().await.unwrap());
        }
        assert_eq!(storage.list(&path).await.unwrap(), before);
        assert_eq!(
            session
                .committer
                .commit(outputs)
                .await
                .unwrap()
                .rows_written,
            0
        );
        let files = storage.list(&path).await.unwrap();
        assert_eq!(files.len(), 1);
        if mode == WriteMode::Append {
            assert_eq!(files, before);
        }
        let batches = open_arrow_sink_test_support::read_parquet_dir(&storage, &directory).await;
        assert!(!batches.is_empty());
        for actual in batches {
            assert_eq!(actual.schema(), batch().schema());
            assert_eq!(actual.num_rows(), 0);
        }
    }
}
