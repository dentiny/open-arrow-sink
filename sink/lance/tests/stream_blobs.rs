use std::sync::Arc;

use arrow_array::{Array, ArrayRef, LargeBinaryArray, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use lance::datatypes::BlobHandling;
use open_arrow_sink_core::{
    BLOB_METADATA_KEY, BlobChunk, BlobPlacement, Ordering, Sink, WriteMode, WriteOptions,
};
use open_arrow_sink_lance::{LanceDatasetSink, Storage};
use open_arrow_sink_storage::opendal;

fn storage() -> Storage {
    Storage::new(
        opendal::Operator::new(opendal::services::Memory::default()).unwrap(),
        "memory:///warehouse/".parse().unwrap(),
    )
    .unwrap()
}
fn input() -> RecordBatch {
    let field = Field::new("blob", DataType::LargeBinary, true)
        .with_metadata([(BLOB_METADATA_KEY.into(), "true".into())].into());
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![field])),
        vec![Arc::new(LargeBinaryArray::from(vec![
            Some(b"".as_slice()),
            None,
            Some(b"".as_slice()),
        ])) as ArrayRef],
    )
    .unwrap()
}
fn options() -> WriteOptions {
    WriteOptions {
        ordering: Ordering::Unordered,
        ..Default::default()
    }
}

#[tokio::test]
async fn shared_storage_streams_large_empty_and_null_blobs() {
    let storage = storage();
    let url = storage.url("雪 output%20/").unwrap();
    let sink = LanceDatasetSink::new(storage);
    let io = open_arrow_sink_core::IoMetrics::default();
    let mut writer = sink
        .open(
            url.clone(),
            input().schema(),
            WriteOptions {
                io_metrics: Some(io.clone()),
                ..options()
            },
        )
        .await
        .unwrap();
    let mut blob = writer.create_blob().await.unwrap();
    // A 17 MiB value streamed from bounded chunks.
    for _ in 0..272 {
        blob.write(BlobChunk::new(vec![b'x'; 64 * 1024]))
            .await
            .unwrap();
    }
    let large = blob.finish().await.unwrap();
    let empty = writer.create_blob().await.unwrap().finish().await.unwrap();
    writer
        .write_with_blobs(
            input(),
            vec![
                BlobPlacement {
                    column: 0,
                    row: 0,
                    blob: large,
                },
                BlobPlacement {
                    column: 0,
                    row: 2,
                    blob: empty,
                },
            ],
        )
        .await
        .unwrap();
    let summary = writer.commit().await.unwrap();
    assert_eq!(summary.rows_written, 3);
    assert!(io.snapshot().bytes_written > 17 * 1024 * 1024);
    let dataset = sink.dataset_builder(url).unwrap().load().await.unwrap();
    let actual = dataset
        .scan()
        .blob_handling(BlobHandling::AllBinary)
        .try_into_batch()
        .await
        .unwrap();
    let values = actual
        .column(0)
        .as_any()
        .downcast_ref::<LargeBinaryArray>()
        .unwrap();
    assert_eq!(values.value(0).len(), 17 * 1024 * 1024);
    assert!(values.value(0).iter().all(|&b| b == b'x'));
    assert!(values.is_null(1));
    assert!(!values.is_null(2));
    assert_eq!(values.value(2), b"");
}

#[tokio::test]
async fn overwrite_abort_removes_attached_session_blobs() {
    let storage = storage();
    let url = storage.url("existing/").unwrap();
    let sink = LanceDatasetSink::new(storage.clone());
    sink.open(url.clone(), input().schema(), options())
        .await
        .unwrap()
        .commit()
        .await
        .unwrap();
    let mut writer = sink
        .open(
            url.clone(),
            input().schema(),
            WriteOptions {
                mode: open_arrow_sink_core::WriteMode::Overwrite,
                ..options()
            },
        )
        .await
        .unwrap();
    let mut blob = writer.create_blob().await.unwrap();
    blob.write(BlobChunk::new("payload")).await.unwrap();
    writer
        .write_with_blobs(
            input(),
            vec![BlobPlacement {
                column: 0,
                row: 0,
                blob: blob.finish().await.unwrap(),
            }],
        )
        .await
        .unwrap();
    writer.abort().await.unwrap();
    assert!(
        storage
            .list(&url.join("_blobs/").unwrap())
            .await
            .unwrap()
            .is_empty()
    );
    let dataset = sink.dataset_builder(url).unwrap().load().await.unwrap();
    assert_eq!(dataset.count_rows(None).await.unwrap(), 0);
}

#[rstest::rstest]
#[case::dropped(true)]
#[case::unfinished(false)]
#[tokio::test]
async fn partial_or_unfinished_blobs_prevent_publication_and_are_cleaned_up(
    #[case] drop_partial: bool,
) {
    let storage = storage();
    let url = storage.url("failed/").unwrap();
    let sink = LanceDatasetSink::new(storage.clone());
    let mut writer = sink
        .open(url.clone(), input().schema(), options())
        .await
        .unwrap();
    let mut blob = Some(writer.create_blob().await.unwrap());
    blob.as_mut()
        .unwrap()
        .write(BlobChunk::new("partial"))
        .await
        .unwrap();
    if drop_partial {
        drop(blob.take());
    }
    assert!(writer.commit().await.is_err());
    assert!(storage.list(&url).await.unwrap().is_empty());
    if let Some(mut blob) = blob {
        assert!(blob.write(BlobChunk::new("late")).await.is_err());
        assert!(blob.finish().await.is_err());
    }
}

#[rstest::rstest]
#[case::create(WriteMode::Create)]
#[case::append(WriteMode::Append)]
#[tokio::test]
async fn writes_to_a_blob_dataset(#[case] mode: WriteMode) {
    let storage = storage();
    let url = storage.url("existing/").unwrap();
    let sink = LanceDatasetSink::new(storage);
    if mode == WriteMode::Append {
        sink.open(url.clone(), input().schema(), options())
            .await
            .unwrap()
            .commit()
            .await
            .unwrap();
    }
    let mut writer = sink
        .open(
            url.clone(),
            input().schema(),
            WriteOptions { mode, ..options() },
        )
        .await
        .unwrap();
    writer.write(input()).await.unwrap();
    writer.commit().await.unwrap();
    let dataset = sink.dataset_builder(url).unwrap().load().await.unwrap();
    assert_eq!(dataset.count_rows(None).await.unwrap(), 3);
}

#[rstest::rstest]
#[case::abort(false)]
#[case::failed_commit(true)]
#[tokio::test]
async fn finished_blob_io_survives_abort_or_failed_commit(#[case] fail_commit: bool) {
    let storage = storage();
    let io = open_arrow_sink_core::IoMetrics::default();
    let mut writer = LanceDatasetSink::new(storage.clone())
        .open(
            storage.url("io-failure").unwrap(),
            input().schema(),
            WriteOptions {
                io_metrics: Some(io.clone()),
                ..options()
            },
        )
        .await
        .unwrap();
    let mut blob = writer.create_blob().await.unwrap();
    blob.write(BlobChunk::new(vec![b'x'; 4096])).await.unwrap();
    let handle = blob.finish().await.unwrap();
    assert_eq!(io.snapshot().bytes_written, 4096);
    drop(handle);
    if fail_commit {
        // Dropping an unfinished blob makes commit fail before publication.
        drop(writer.create_blob().await.unwrap());
        assert!(writer.commit().await.is_err());
    } else {
        writer.abort().await.unwrap();
    }
    assert!(io.snapshot().bytes_written >= 4096);
}

#[rstest::rstest]
#[case::default_dedicated(None, None, 4, false)]
#[case::inline_boundary(Some(4), None, 4, true)]
#[case::spill_after_boundary(Some(4), None, 5, false)]
#[case::empty_inline(Some(0), None, 0, true)]
#[case::dedicated_precedence(Some(8), Some(2), 3, false)]
#[tokio::test]
async fn streamed_inline_threshold_avoids_objects_and_spills_larger_payloads(
    #[case] inline_limit: Option<usize>,
    #[case] dedicated_limit: Option<usize>,
    #[case] payload_size: usize,
    #[case] inline: bool,
) {
    use lance::datatypes::BlobKind;
    use open_arrow_sink_core::BlobHandleConfig;
    use std::num::NonZeroUsize;

    let storage = storage();
    let url = storage.url("layout/").unwrap();
    let sink = LanceDatasetSink::new(storage.clone());
    let io = open_arrow_sink_core::IoMetrics::default();
    let mut config = options();
    config.io_metrics = Some(io.clone());
    config.blob_handle = Some(BlobHandleConfig {
        inline_size_threshold: inline_limit,
        dedicated_size_threshold: dedicated_limit.and_then(NonZeroUsize::new),
        ..Default::default()
    });
    let mut writer = sink
        .open(url.clone(), input().schema(), config)
        .await
        .unwrap();
    let mut blob = writer.create_blob().await.unwrap();
    let payload: Vec<_> = (0..payload_size).map(|i| i as u8).collect();
    // Multiple chunks exercise promotion without losing the buffered prefix.
    for chunk in payload.chunks(2) {
        blob.write(BlobChunk::new(chunk.to_vec())).await.unwrap();
    }
    let handle = blob.finish().await.unwrap();
    assert_eq!(
        io.snapshot().bytes_written,
        if inline { 0 } else { payload_size as u64 }
    );
    writer
        .write_with_blobs(
            input(),
            vec![BlobPlacement {
                column: 0,
                row: 0,
                blob: handle,
            }],
        )
        .await
        .unwrap();
    writer.commit().await.unwrap();
    assert!(io.snapshot().bytes_written > 0);
    let objects = storage.list(&url.join("_blobs/").unwrap()).await.unwrap();
    assert_eq!(objects.len(), usize::from(!inline));
    let dataset = Arc::new(sink.dataset_builder(url).unwrap().load().await.unwrap());
    let blobs = dataset.take_blobs_by_indices(&[0], "blob").await.unwrap();
    let blob = blobs[0].as_ref().unwrap();
    assert_eq!(
        blob.kind(),
        if inline {
            BlobKind::Inline
        } else {
            BlobKind::Managed
        }
    );
    assert_eq!(blob.read().await.unwrap().as_ref(), payload.as_slice());
}

#[tokio::test]
async fn abandoning_buffered_inline_blob_still_prevents_publication() {
    use open_arrow_sink_core::BlobHandleConfig;
    let storage = storage();
    let url = storage.url("abandoned-inline/").unwrap();
    let mut config = options();
    config.blob_handle = Some(BlobHandleConfig {
        inline_size_threshold: Some(16),
        ..Default::default()
    });
    let mut writer = LanceDatasetSink::new(storage.clone())
        .open(url.clone(), input().schema(), config)
        .await
        .unwrap();
    let mut blob = writer.create_blob().await.unwrap();
    blob.write(BlobChunk::new("partial")).await.unwrap();
    drop(blob);
    assert!(writer.commit().await.is_err());
    assert!(storage.list(&url).await.unwrap().is_empty());
}
