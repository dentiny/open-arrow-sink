use std::sync::Arc;

use arrow_array::{Array, ArrayRef, Int64Array, RecordBatch};
use lance::Dataset;
use open_arrow_sink_core::{Ordering, Sink, WriteOptions};
use open_arrow_sink_lance::LanceDatasetSink;

fn batch() -> RecordBatch {
    RecordBatch::try_from_iter([(
        "value",
        Arc::new(Int64Array::from(vec![Some(3), None, Some(1), Some(3)])) as ArrayRef,
    )])
    .unwrap()
}

fn options() -> WriteOptions {
    WriteOptions {
        ordering: Ordering::Unordered,
        ..WriteOptions::default()
    }
}

#[tokio::test]
async fn commit_round_trips_multiple_batches() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("dataset");
    let expected = batch();
    let mut writer = LanceDatasetSink
        .open(path.clone(), expected.schema(), options())
        .await
        .unwrap();
    assert_eq!(writer.schema(), &expected.schema());
    writer.write(expected.slice(0, 1)).await.unwrap();
    writer.write(expected.slice(1, 3)).await.unwrap();
    assert_eq!(writer.commit().await.unwrap().rows_written, 4);
    let dataset = Dataset::open(path.to_str().unwrap()).await.unwrap();
    let actual = dataset.scan().try_into_batch().await.unwrap();
    assert_eq!(actual.schema(), expected.schema());
    let values = actual
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let mut values: Vec<_> = (0..values.len())
        .map(|i| (!values.is_null(i)).then(|| values.value(i)))
        .collect();
    values.sort_unstable();
    assert_eq!(values, vec![None, Some(1), Some(3), Some(3)]);
}

#[tokio::test]
async fn empty_commit_produces_readable_dataset() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("dataset");
    let schema = batch().schema();
    let writer = LanceDatasetSink
        .open(path.clone(), schema.clone(), options())
        .await
        .unwrap();
    assert_eq!(writer.commit().await.unwrap().rows_written, 0);
    let dataset = Dataset::open(path.to_str().unwrap()).await.unwrap();
    assert_eq!(dataset.count_rows(None).await.unwrap(), 0);
    assert_eq!(
        dataset.scan().try_into_batch().await.unwrap().schema(),
        schema
    );
}

#[tokio::test]
async fn abort_removes_dataset_and_preserves_sibling_files() {
    let temp = tempfile::tempdir().unwrap();
    let sibling = temp.path().join("keep.txt");
    tokio::fs::write(&sibling, "keep").await.unwrap();
    let path = temp.path().join("dataset");
    let batch = batch();
    let mut writer = LanceDatasetSink
        .open(path.clone(), batch.schema(), options())
        .await
        .unwrap();
    writer.write(batch).await.unwrap();
    writer.abort().await.unwrap();
    assert!(!tokio::fs::try_exists(&path).await.unwrap());
    assert_eq!(tokio::fs::read_to_string(sibling).await.unwrap(), "keep");
}

#[rstest::rstest]
#[case::default_size(None, false)]
#[case::small_target(Some(1), true)]
#[tokio::test]
async fn target_file_size_rolls_files_without_losing_rows(
    #[case] target_file_size: Option<usize>,
    #[case] should_roll: bool,
) {
    // Exceed the SDK's default page buffer while staying below its row-count limit.
    let count = 10 * 1024;
    let payloads = arrow_array::FixedSizeBinaryArray::try_from_iter((0..count).map(|i| {
        let mut state = i as u64 + 1;
        let mut bytes = [0; 1024];
        for byte in &mut bytes {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            *byte = state as u8;
        }
        bytes
    }))
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
    let mut writer = LanceDatasetSink
        .open(
            path.clone(),
            expected.schema(),
            WriteOptions {
                target_file_size,
                ..options()
            },
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
    if should_roll {
        more_asserts::assert_gt!(fragments.len(), 1);
    } else {
        assert_eq!(fragments.len(), 1);
    }
    let actual = dataset.scan().try_into_batch().await.unwrap();
    assert_eq!(actual.schema(), expected.schema());
    let ids = actual
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let payloads = actual
        .column(1)
        .as_any()
        .downcast_ref::<arrow_array::FixedSizeBinaryArray>()
        .unwrap();
    let expected_payloads = expected
        .column(1)
        .as_any()
        .downcast_ref::<arrow_array::FixedSizeBinaryArray>()
        .unwrap();
    let mut occurrences = vec![0; count];
    for i in 0..actual.num_rows() {
        let id = usize::try_from(ids.value(i)).unwrap();
        assert_eq!(payloads.value(i), expected_payloads.value(id));
        occurrences[id] += 1;
    }
    assert_eq!(occurrences, vec![2; count]);
}

#[tokio::test]
async fn parallel_fragments_stay_unpublished_until_one_session_commit() {
    use open_arrow_sink_core::ParallelSink;
    use std::{collections::HashSet, num::NonZeroUsize};
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("parallel");
    let expected = batch();
    let mut session = LanceDatasetSink
        .open_writers(
            path.clone(),
            expected.schema(),
            options(),
            NonZeroUsize::new(3).unwrap(),
        )
        .await
        .unwrap();
    let mut outputs = Vec::new();
    for writer in &mut session.writers {
        writer.write(expected.clone()).await.unwrap();
        outputs.push(writer.finish().await.unwrap());
    }
    assert!(Dataset::open(path.to_str().unwrap()).await.is_err());
    assert_eq!(
        session
            .committer
            .commit(outputs)
            .await
            .unwrap()
            .rows_written,
        12
    );
    let dataset = Dataset::open(path.to_str().unwrap()).await.unwrap();
    assert_eq!(dataset.version_id(), 1);
    assert_eq!(dataset.count_rows(None).await.unwrap(), 12);
    assert_eq!(dataset.get_fragments().len(), 3);
    let ids: HashSet<_> = dataset.get_fragments().iter().map(|f| f.id()).collect();
    assert_eq!(ids.len(), 3);
    let actual = dataset.scan().try_into_batch().await.unwrap();
    let values = actual
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let mut actual: Vec<_> = values.iter().collect();
    actual.sort_unstable();
    let mut expected = [None, Some(1), Some(3), Some(3)].repeat(3);
    expected.sort_unstable();
    assert_eq!(actual, expected);
}

#[tokio::test]
async fn unordered_lance_layer_handles_empty_and_nonempty_sessions() {
    use open_arrow_sink_core::SinkExt;
    use open_arrow_sink_layer::UnorderedWriteLayer;
    use std::num::NonZeroUsize;
    for batches in [0, 8] {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("parallel");
        let sink = LanceDatasetSink.layer(UnorderedWriteLayer::new(NonZeroUsize::new(3).unwrap()));
        let mut writer = sink
            .open(path.clone(), batch().schema(), options())
            .await
            .unwrap();
        for _ in 0..batches {
            writer.write(batch()).await.unwrap();
        }
        assert_eq!(writer.commit().await.unwrap().rows_written, batches * 4);
        let dataset = Dataset::open(path.to_str().unwrap()).await.unwrap();
        assert_eq!(dataset.version_id(), 1);
        assert_eq!(
            dataset.count_rows(None).await.unwrap(),
            batches as usize * 4
        );
    }
}

#[tokio::test]
async fn unordered_lance_abort_and_schema_failure_honor_cleanup_policy() {
    use open_arrow_sink_core::SinkExt;
    use open_arrow_sink_layer::UnorderedWriteLayer;
    use std::num::NonZeroUsize;
    for cleanup in [false, true] {
        for fail in [false, true] {
            let temp = tempfile::tempdir().unwrap();
            let sibling = temp.path().join("keep");
            tokio::fs::write(&sibling, "keep").await.unwrap();
            let path = temp.path().join("parallel");
            let sink =
                LanceDatasetSink.layer(UnorderedWriteLayer::new(NonZeroUsize::new(3).unwrap()));
            let mut writer = sink
                .open(
                    path.clone(),
                    batch().schema(),
                    WriteOptions {
                        cleanup_on_error: cleanup,
                        ..options()
                    },
                )
                .await
                .unwrap();
            writer.write(batch()).await.unwrap();
            if fail {
                let bad = RecordBatch::try_from_iter([(
                    "wrong",
                    Arc::new(Int64Array::from(vec![1])) as ArrayRef,
                )])
                .unwrap();
                assert!(writer.write(bad).await.is_err());
                assert!(writer.commit().await.is_err());
            } else {
                writer.abort().await.unwrap();
            }
            assert_eq!(tokio::fs::try_exists(&path).await.unwrap(), !cleanup);
            assert!(Dataset::open(path.to_str().unwrap()).await.is_err());
            assert_eq!(tokio::fs::read_to_string(&sibling).await.unwrap(), "keep");
        }
    }
}
