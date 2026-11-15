use std::{num::NonZeroUsize, sync::Arc};

use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use open_arrow_sink_core::{Ordering, Sink, SinkExt, WriteOptions};
use open_arrow_sink_layer::UnorderedWriteLayer;
use open_arrow_sink_parquet::{ParquetDirectorySink, Storage};
use open_arrow_sink_storage::opendal;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

#[tokio::test]
async fn unordered_commit_preserves_all_rows() {
    let storage = Storage::new(
        opendal::Operator::new(opendal::services::Memory::default()).unwrap(),
        "memory:///warehouse/".parse().unwrap(),
    )
    .unwrap();
    let directory = storage.url("雪 output/").unwrap();
    let make_batch = |id| {
        RecordBatch::try_from_iter([
            (
                "id",
                Arc::new(Int64Array::from(vec![Some(id * 2), None, Some(id * 2 + 1)])) as ArrayRef,
            ),
            (
                "text",
                Arc::new(StringArray::from(vec![Some("雪"), Some(""), None])) as ArrayRef,
            ),
        ])
        .unwrap()
    };
    let mut writer = ParquetDirectorySink::new(storage.clone())
        .layer(UnorderedWriteLayer::new(NonZeroUsize::new(3).unwrap()))
        .open(
            directory.clone(),
            make_batch(0).schema(),
            WriteOptions {
                ordering: Ordering::Unordered,
                target_file_size: Some(1),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let mut expected = Vec::new();
    for id in 0..8 {
        writer.write(make_batch(id)).await.unwrap();
        expected.extend([
            (Some(id * 2), Some("雪".to_owned())),
            (None, Some(String::new())),
            (Some(id * 2 + 1), None),
        ]);
    }
    assert_eq!(
        writer.commit().await.unwrap().rows_written,
        expected.len() as u64
    );
    let mut actual = Vec::new();
    for file in storage.list(&directory).await.unwrap() {
        assert!(file.path().ends_with(".parquet"));
        for batch in ParquetRecordBatchReaderBuilder::try_new(storage.read(&file).await.unwrap())
            .unwrap()
            .build()
            .unwrap()
        {
            let batch = batch.unwrap();
            let ids = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let text = batch
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            actual.extend(
                ids.iter()
                    .zip(text.iter().map(|value| value.map(str::to_owned))),
            );
        }
    }
    actual.sort_unstable();
    expected.sort_unstable();
    assert_eq!(actual, expected);
    assert!(
        ParquetDirectorySink::new(storage)
            .open(
                directory,
                make_batch(0).schema(),
                WriteOptions {
                    ordering: Ordering::Unordered,
                    ..Default::default()
                }
            )
            .await
            .is_err()
    );
}
