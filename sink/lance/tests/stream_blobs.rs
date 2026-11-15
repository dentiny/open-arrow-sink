use std::sync::Arc;

use arrow_array::{Array, ArrayRef, LargeBinaryArray, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use lance::datatypes::BlobHandling;
use open_arrow_sink_core::{
    BLOB_METADATA_KEY, BlobChunk, BlobPlacement, Ordering, Sink, WriteOptions,
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
    let mut writer = sink
        .open(url.clone(), input().schema(), options())
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
    assert_eq!(writer.commit().await.unwrap().rows_written, 3);
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
async fn partial_or_unfinished_blobs_prevent_publication_and_are_cleaned_up() {
    for drop_partial in [true, false] {
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
}
