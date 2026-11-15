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

#[tokio::test]
async fn shared_operator_commit_indexes_and_read_back() {
    use lance::index::DatasetIndexExt;
    let storage = storage();
    let sink = LanceDatasetSink::new(storage.clone());
    let url = storage.url("雪 output%20/").unwrap();
    let options = WriteOptions {
        indexes: vec![IndexSpec::new("value", IndexType::Scalar)],
        ..write_options()
    };
    let mut writer = sink
        .clone()
        .layer(UnorderedWriteLayer::new(NonZeroUsize::new(3).unwrap()))
        .open(url.clone(), batch().schema(), options)
        .await
        .unwrap();
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

#[tokio::test]
async fn shared_operator_abort_honors_cleanup() {
    for cleanup_on_error in [true, false] {
        let storage = storage();
        let sink = LanceDatasetSink::new(storage.clone())
            .layer(UnorderedWriteLayer::new(NonZeroUsize::new(3).unwrap()));
        let url = storage.url("aborted/").unwrap();
        let mut writer = sink
            .open(
                url.clone(),
                batch().schema(),
                WriteOptions {
                    cleanup_on_error,
                    ..write_options()
                },
            )
            .await
            .unwrap();
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
}

#[tokio::test]
async fn explicit_storage_version() {
    let storage = storage();
    let sink = LanceDatasetSink::new(storage.clone());
    assert!(sink.capability().supports_storage_version);
    let url = storage.url("versioned/").unwrap();
    let mut writer = sink
        .open(
            url.clone(),
            batch().schema(),
            WriteOptions {
                storage_version: Some("2.1".into()),
                ..write_options()
            },
        )
        .await
        .unwrap();
    writer.write(batch()).await.unwrap();
    writer.commit().await.unwrap();
    let dataset = sink.dataset_builder(url).unwrap().load().await.unwrap();
    assert_eq!(
        dataset.manifest().data_storage_format.version.to_string(),
        "2.1"
    );
}

#[tokio::test]
async fn idle_writer_does_not_supply_dictionary_schema() {
    use arrow_array::{DictionaryArray, types::Int32Type};
    use open_arrow_sink_core::ParallelSink;
    let storage = storage();
    let sink = LanceDatasetSink::new(storage.clone());
    let url = storage.url("dictionary/").unwrap();
    let values: DictionaryArray<Int32Type> = ["hello"].into_iter().collect();
    let batch = RecordBatch::try_from_iter([("value", Arc::new(values) as ArrayRef)]).unwrap();
    let mut session = sink
        .open_writers(
            url.clone(),
            batch.schema(),
            WriteOptions {
                storage_version: Some("legacy".into()),
                ..write_options()
            },
            NonZeroUsize::new(2).unwrap(),
        )
        .await
        .unwrap();
    session.writers[1].write(batch).await.unwrap();
    let mut outputs = Vec::new();
    for writer in &mut session.writers {
        outputs.push(writer.finish().await.unwrap());
    }
    session.committer.commit(outputs).await.unwrap();
    let dataset = sink.dataset_builder(url).unwrap().load().await.unwrap();
    assert_eq!(dataset.scan().try_into_batch().await.unwrap().num_rows(), 1);
}
