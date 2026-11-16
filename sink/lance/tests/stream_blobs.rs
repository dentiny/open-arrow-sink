use arrow_array::cast::AsArray;
use std::sync::Arc;

use arrow_array::{Array, ArrayRef, LargeBinaryArray, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use lance::datatypes::BlobHandling;
use open_arrow_sink_core::{BLOB_METADATA_KEY, BlobChunk, BlobPlacement, Sink, WriteMode};
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
use open_arrow_sink_test_support::unordered_options as options;

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
            options().with_io_metrics(Some(io.clone())),
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
    let values = actual.column(0).as_binary::<i64>();
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
            options().with_mode(open_arrow_sink_core::WriteMode::Overwrite),
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
        .open(url.clone(), input().schema(), options().with_mode(mode))
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
            options().with_io_metrics(Some(io.clone())),
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
async fn streamed_blobs_keep_compression_nested_columns_and_input_schema() {
    use arrow_array::{Int64Array, StructArray, cast::AsArray, types::Int64Type};
    use open_arrow_sink_core::{Compression, CompressionOptions};

    let blob = Field::new("blob", DataType::LargeBinary, true)
        .with_metadata([(BLOB_METADATA_KEY.into(), "true".into())].into());
    let value = Arc::new(Field::new("value", DataType::Int64, false));
    let nested = Arc::new(StructArray::from(vec![(
        value.clone(),
        Arc::new(Int64Array::from(vec![11, 22])) as ArrayRef,
    )]));
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            blob,
            Field::new("nested", DataType::Struct(vec![value].into()), false),
            Field::new("compressed", DataType::Int64, false),
        ])),
        vec![
            Arc::new(LargeBinaryArray::from(vec![
                Some(b"inline".as_slice()),
                None,
            ])) as ArrayRef,
            nested,
            Arc::new(Int64Array::from(vec![33, 44])),
        ],
    )
    .unwrap();
    let storage = storage();
    let url = storage.url("nested-compression/").unwrap();
    let sink = LanceDatasetSink::new(storage);
    let mut writer = sink
        .open(
            url.clone(),
            batch.schema(),
            WriteOptions {
                compression: CompressionOptions {
                    columns: [("compressed".into(), Compression::Zstd { level: Some(1) })].into(),
                    nested_columns: [(
                        vec!["nested".into(), "value".into()],
                        Compression::Zstd { level: Some(1) },
                    )]
                    .into(),
                    ..Default::default()
                },
                ..options()
            },
        )
        .await
        .unwrap();
    assert_eq!(writer.schema(), &batch.schema());
    writer.write(batch).await.unwrap();
    assert_eq!(writer.commit().await.unwrap().rows_written, 2);

    let dataset = sink.dataset_builder(url).unwrap().load().await.unwrap();
    let actual = dataset
        .scan()
        .blob_handling(BlobHandling::AllBinary)
        .try_into_batch()
        .await
        .unwrap();
    let blobs = actual.column(0).as_binary::<i64>();
    assert_eq!(blobs.value(0), b"inline");
    assert!(blobs.is_null(1));
    let values = actual
        .column(1)
        .as_struct()
        .column(0)
        .as_primitive::<Int64Type>();
    assert_eq!(values.values(), &[11, 22]);
    let compressed = actual.column(2).as_primitive::<Int64Type>();
    assert_eq!(compressed.values(), &[33, 44]);
    assert_eq!(
        actual.schema().field(2).metadata()["lance-encoding:compression"],
        "zstd"
    );
}
