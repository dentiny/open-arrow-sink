use arrow_array::{ArrayRef, Int64Array, RecordBatch};
use open_arrow_sink_core::{IndexSpec, IndexType, Ordering, Sink, SinkExt, WriteOptions};
use open_arrow_sink_lance::{LanceDatasetSink, Storage};
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
    let mut writer = if count == 1 {
        sink.open(url.clone(), batch().schema(), options)
            .await
            .unwrap()
    } else {
        sink.clone()
            .layer(UnorderedWriteLayer::new(NonZeroUsize::new(count).unwrap()))
            .open(url.clone(), batch().schema(), options)
            .await
            .unwrap()
    };
    for _ in 0..4 {
        writer.write(batch()).await.unwrap();
    }
    assert_eq!(writer.commit().await.unwrap().rows_written, 12);
    let dataset = sink
        .dataset_builder(url.clone())
        .unwrap()
        .load()
        .await
        .unwrap();
    assert_eq!(dataset.count_rows(None).await.unwrap(), 12);
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

#[rstest::rstest]
#[case::single(1)]
#[case::parallel(3)]
#[tokio::test]
async fn shared_operator_empty_commit(#[case] count: usize) {
    let storage = storage();
    let sink = LanceDatasetSink::new(storage.clone());
    let url = storage.url("empty/").unwrap();
    let writer = if count == 1 {
        sink.open(url.clone(), batch().schema(), write_options())
            .await
            .unwrap()
    } else {
        sink.clone()
            .layer(UnorderedWriteLayer::new(NonZeroUsize::new(count).unwrap()))
            .open(url.clone(), batch().schema(), write_options())
            .await
            .unwrap()
    };
    assert_eq!(writer.commit().await.unwrap().rows_written, 0);
    assert_eq!(
        sink.dataset_builder(url)
            .unwrap()
            .load()
            .await
            .unwrap()
            .count_rows(None)
            .await
            .unwrap(),
        0
    );
}

#[rstest::rstest]
#[case::single_cleanup(1, true)]
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
    let mut writer = if count == 1 {
        sink.open(url.clone(), batch().schema(), options)
            .await
            .unwrap()
    } else {
        sink.layer(UnorderedWriteLayer::new(NonZeroUsize::new(count).unwrap()))
            .open(url.clone(), batch().schema(), options)
            .await
            .unwrap()
    };
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
