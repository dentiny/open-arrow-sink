use std::{num::NonZeroUsize, sync::Arc};

use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use futures::TryStreamExt;
use open_arrow_sink_core::{Ordering, Sink, SinkExt, WriteOptions};
use open_arrow_sink_layer::UnorderedWriteLayer;
use open_arrow_sink_parquet::{ParquetDirectorySink, Url};
use parquet::arrow::ParquetRecordBatchStreamBuilder;

#[tokio::test]
async fn unordered_commit_preserves_all_rows() {
    let temp = tempfile::tempdir().unwrap();
    let directory = temp.path().join("output");
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
    let mut writer = ParquetDirectorySink
        .layer(UnorderedWriteLayer::new(NonZeroUsize::new(3).unwrap()))
        .open(
            Url::from_directory_path(&directory).unwrap(),
            make_batch(0).schema(),
            WriteOptions {
                ordering: Ordering::Unordered,
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
    let mut files = tokio::fs::read_dir(&directory).await.unwrap();
    while let Some(entry) = files.next_entry().await.unwrap() {
        let file = tokio::fs::File::open(entry.path()).await.unwrap();
        let mut batches = ParquetRecordBatchStreamBuilder::new(file)
            .await
            .unwrap()
            .build()
            .unwrap();
        while let Some(batch) = batches.try_next().await.unwrap() {
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
}
