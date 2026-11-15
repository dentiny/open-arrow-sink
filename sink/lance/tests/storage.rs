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

#[rstest::rstest]
#[case::sequential(1)]
#[case::parallel(3)]
#[tokio::test]
async fn explicit_storage_version_round_trips(#[case] writers: usize) {
    let storage = storage();
    let sink = LanceDatasetSink::new(storage.clone());
    assert!(sink.capability().supports_storage_version);
    let url = storage.url("versioned/").unwrap();
    let layered = sink.clone().layer(UnorderedWriteLayer::new(
        NonZeroUsize::new(writers).unwrap(),
    ));
    use open_arrow_sink_core::WriteMode::{Append, Create, Overwrite};
    let mut previous_fragments = 0;
    for (mode, version, expected_default, expected_file, expected_rows) in [
        (Create, Some("2.0"), "2.0", "2.0", 9),
        (Append, None, "2.0", "2.0", 18),
        (Append, Some("2.1"), "2.0", "2.1", 27),
        (Overwrite, Some("2.1"), "2.1", "2.1", 9),
        (Append, None, "2.1", "2.1", 18),
    ] {
        let mut writer = layered
            .open(
                url.clone(),
                batch().schema(),
                WriteOptions {
                    mode,
                    storage_version: version.map(str::to_owned),
                    ..write_options()
                },
            )
            .await
            .unwrap();
        for _ in 0..3 {
            writer.write(batch()).await.unwrap();
        }
        writer.commit().await.unwrap();
        let dataset = sink
            .dataset_builder(url.clone())
            .unwrap()
            .load()
            .await
            .unwrap();
        assert_eq!(
            dataset.manifest().data_storage_format.version.to_string(),
            expected_default
        );
        let skip = if mode == Append {
            previous_fragments
        } else {
            0
        };
        let new_fragments = &dataset.fragments()[skip..];
        assert!(!new_fragments.is_empty());
        for file in new_fragments.iter().flat_map(|fragment| &fragment.files) {
            assert_eq!(file.file_version().unwrap().to_string(), expected_file);
        }
        previous_fragments = dataset.fragments().len();
        assert_eq!(dataset.count_rows(None).await.unwrap(), expected_rows);
        assert_eq!(
            dataset.scan().try_into_batch().await.unwrap().num_rows(),
            expected_rows
        );
    }
}

#[tokio::test]
async fn invalid_storage_version_fails_before_destination_io() {
    let storage = storage();
    let sink = LanceDatasetSink::new(storage.clone());
    for version in ["", "invalid", "99.0"] {
        let url = storage.url("invalid-version/").unwrap();
        let result = sink
            .open(
                url.clone(),
                batch().schema(),
                WriteOptions {
                    storage_version: Some(version.into()),
                    ..write_options()
                },
            )
            .await;
        assert!(
            matches!(result, Err(open_arrow_sink_core::Error::Unsupported(message)) if message.contains("storage version"))
        );
        assert!(storage.list(&url).await.unwrap().is_empty());
    }
}

#[tokio::test]
async fn empty_commit_keeps_explicit_storage_version() {
    let storage = storage();
    let sink = LanceDatasetSink::new(storage.clone());
    let url = storage.url("empty-versioned/").unwrap();
    for (mode, version) in [
        (open_arrow_sink_core::WriteMode::Create, "2.0"),
        (open_arrow_sink_core::WriteMode::Overwrite, "2.1"),
    ] {
        let writer = sink
            .open(
                url.clone(),
                batch().schema(),
                WriteOptions {
                    mode,
                    storage_version: Some(version.into()),
                    ..write_options()
                },
            )
            .await
            .unwrap();
        assert_eq!(writer.commit().await.unwrap().rows_written, 0);
        let dataset = sink
            .dataset_builder(url.clone())
            .unwrap()
            .load()
            .await
            .unwrap();
        assert_eq!(
            dataset.manifest().data_storage_format.version.to_string(),
            version
        );
        assert_eq!(dataset.count_rows(None).await.unwrap(), 0);
    }
}
