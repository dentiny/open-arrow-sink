use arrow_array::{ArrayRef, Int64Array, RecordBatch};
use open_arrow_sink_core::{
    IndexSpec, IndexType, Ordering, Sink, SinkExt, WriteMode, WriteOptions, Writer,
};
use open_arrow_sink_lance::{LanceDatasetSink, Storage, Url};
use open_arrow_sink_layer::UnorderedWriteLayer;
use open_arrow_sink_storage::opendal;
use std::{num::NonZeroUsize, sync::Arc};

fn storage() -> Storage {
    Storage::new(
        opendal::Operator::new(opendal::services::Memory::default()).unwrap(),
        "memory:///warehouse/".parse().unwrap(),
    )
    .unwrap()
}
fn batch() -> RecordBatch {
    RecordBatch::try_from_iter([(
        "value",
        Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef,
    )])
    .unwrap()
}
fn write_options() -> WriteOptions {
    WriteOptions {
        ordering: Ordering::Unordered,
        ..Default::default()
    }
}

async fn open(
    sink: &LanceDatasetSink,
    url: &Url,
    count: usize,
    options: WriteOptions,
) -> Box<dyn Writer> {
    if count == 1 {
        sink.open(url.clone(), batch().schema(), options)
            .await
            .unwrap()
    } else {
        sink.clone()
            .layer(UnorderedWriteLayer::new(NonZeroUsize::new(count).unwrap()))
            .open(url.clone(), batch().schema(), options)
            .await
            .unwrap()
    }
}

async fn dataset(sink: &LanceDatasetSink, url: &Url) -> lance::Dataset {
    sink.dataset_builder(url.clone())
        .unwrap()
        .load()
        .await
        .unwrap()
}

#[rstest::rstest]
#[case::single(1)]
#[case::parallel(3)]
#[tokio::test]
async fn shared_operator_commit_indexes_and_read_back(#[case] count: usize) {
    use lance::index::DatasetIndexExt;
    let storage = storage();
    let sink = LanceDatasetSink::new(storage.clone());
    let url = storage.url("雪 output%20/").unwrap();
    let options = WriteOptions {
        indexes: vec![IndexSpec::new("value", IndexType::Scalar)],
        ..write_options()
    };
    let mut writer = open(&sink, &url, count, options).await;
    for _ in 0..4 {
        writer.write(batch()).await.unwrap();
    }
    assert_eq!(writer.commit().await.unwrap().rows_written, 12);
    let dataset = dataset(&sink, &url).await;
    let actual = dataset.scan().try_into_batch().await.unwrap();
    let mut values = actual
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .values()
        .to_vec();
    values.sort_unstable();
    assert_eq!(values, vec![1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3]);
    assert_eq!(dataset.load_indices().await.unwrap().len(), 1);
    // Data and the reservation must resolve to the same physical namespace.
    assert!(storage.list(&url).await.unwrap().len() > 1);
    assert!(
        sink.open(url, batch().schema(), write_options())
            .await
            .is_err()
    );
}

// Single-writer empty publication is already covered by tests/writer.rs.
#[tokio::test]
async fn parallel_empty_commit() {
    let storage = storage();
    let sink = LanceDatasetSink::new(storage.clone());
    let url = storage.url("empty/").unwrap();
    let writer = open(&sink, &url, 3, write_options()).await;
    assert_eq!(writer.commit().await.unwrap().rows_written, 0);
    assert_eq!(
        dataset(&sink, &url).await.count_rows(None).await.unwrap(),
        0
    );
}

#[rstest::rstest]
#[case::single_keep(1, false)]
#[case::parallel_cleanup(3, true)]
#[case::parallel_keep(3, false)]
#[tokio::test]
async fn shared_operator_abort_honors_cleanup(
    #[case] count: usize,
    #[case] cleanup_on_error: bool,
) {
    let storage = storage();
    let sink = LanceDatasetSink::new(storage.clone());
    let url = storage.url("aborted/").unwrap();
    let options = WriteOptions {
        cleanup_on_error,
        ..write_options()
    };
    let mut writer = open(&sink, &url, count, options).await;
    writer.write(batch()).await.unwrap();
    let sibling = storage.url("keep").unwrap();
    storage.write(&sibling, "keep".into()).await.unwrap();
    writer.abort().await.unwrap();
    assert_eq!(
        storage.list(&url).await.unwrap().is_empty(),
        cleanup_on_error
    );
    assert_eq!(storage.read(&sibling).await.unwrap(), "keep");
}

#[rstest::rstest]
#[case::append(WriteMode::Append, 6)]
#[case::overwrite(WriteMode::Overwrite, 3)]
#[tokio::test]
async fn remote_write_modes_preserve_native_commits(
    #[case] mode: WriteMode,
    #[case] expected_rows: usize,
) {
    let storage = storage();
    let sink = LanceDatasetSink::new(storage.clone());
    let url = storage.url("modes/").unwrap();
    let mut seed = open(&sink, &url, 1, write_options()).await;
    seed.write(batch()).await.unwrap();
    seed.commit().await.unwrap();
    let snapshot = dataset(&sink, &url).await;
    let options = WriteOptions {
        mode,
        ..write_options()
    };
    let mut writer = open(&sink, &url, 3, options).await;
    writer.write(batch()).await.unwrap();
    writer.commit().await.unwrap();
    assert_eq!(
        dataset(&sink, &url).await.count_rows(None).await.unwrap(),
        expected_rows
    );
    assert_eq!(snapshot.count_rows(None).await.unwrap(), 3);
}

#[rstest::rstest]
#[case::append(WriteMode::Append)]
#[case::overwrite(WriteMode::Overwrite)]
#[tokio::test]
async fn abort_existing_remote_dataset_retains_committed_rows(#[case] mode: WriteMode) {
    let storage = storage();
    let sink = LanceDatasetSink::new(storage.clone());
    let url = storage.url("abort-existing/").unwrap();
    let mut seed = open(&sink, &url, 1, write_options()).await;
    seed.write(batch()).await.unwrap();
    seed.commit().await.unwrap();
    let mut writer = open(
        &sink,
        &url,
        3,
        WriteOptions {
            mode,
            ..write_options()
        },
    )
    .await;
    writer.write(batch()).await.unwrap();
    writer.abort().await.unwrap();
    assert_eq!(
        dataset(&sink, &url).await.count_rows(None).await.unwrap(),
        3
    );
}
