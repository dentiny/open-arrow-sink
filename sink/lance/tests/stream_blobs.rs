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

#[tokio::test]
async fn explicit_blob_abort_never_clears_another_blob_failure() {
    use open_arrow_sink_core::BlobChunk;
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("aborted");
    let schema = Arc::new(arrow_schema::Schema::new(vec![
        arrow_schema::Field::new("blob", arrow_schema::DataType::LargeBinary, true).with_metadata(
            [(
                open_arrow_sink_core::BLOB_METADATA_KEY.into(),
                "true".into(),
            )]
            .into(),
        ),
    ]));
    let mut options = WriteOptions::default();
    options.ordering = Ordering::Unordered;
    let mut writer = LanceDatasetSink::default()
        .open(
            open_arrow_sink_lance::Url::from_directory_path(path).unwrap(),
            schema,
            options,
        )
        .await
        .unwrap();
    let mut healthy = writer.create_blob().await.unwrap();
    healthy.write(BlobChunk::new("healthy")).await.unwrap();
    let failed = writer.create_blob().await.unwrap();
    drop(failed);
    healthy.abort().await.unwrap();
    assert!(writer.commit().await.is_err());
}
