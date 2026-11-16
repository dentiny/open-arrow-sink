use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatch, StringArray};
use lance::index::DatasetIndexExt;
use open_arrow_sink_core::{
    ExistingIndexPolicy, IndexSpec, IndexType, Ordering, Sink, WriteMode, WriteOptions,
};
use open_arrow_sink_lance::{LanceDatasetSink, Storage};
use open_arrow_sink_storage::opendal;

#[tokio::test]
async fn btree_and_text_indexes_share_one_column_and_keep_their_append_policies() {
    let storage = Storage::new(
        opendal::Operator::new(opendal::services::Memory::default()).unwrap(),
        "memory:///multiple-indexes/".parse().unwrap(),
    )
    .unwrap();
    let sink = LanceDatasetSink::new(storage.clone());
    let url = storage.url("dataset/").unwrap();
    let batch = RecordBatch::try_from_iter([(
        "text",
        Arc::new(StringArray::from(vec![
            "hello world",
            "hello arrow",
            "other",
        ])) as ArrayRef,
    )])
    .unwrap();
    let indexes = vec![
        IndexSpec::new("text", IndexType::Scalar),
        IndexSpec::new("text", IndexType::Text),
    ];
    let mut options = WriteOptions::default();
    options.ordering = Ordering::Unordered;
    options.indexes = indexes.clone();
    let mut writer = sink
        .open(url.clone(), batch.schema(), options.clone())
        .await
        .unwrap();
    writer.write(batch.clone()).await.unwrap();
    assert_eq!(writer.commit().await.unwrap().rows_written, 3);

    let dataset = sink
        .dataset_builder(url.clone())
        .unwrap()
        .load()
        .await
        .unwrap();
    let existing = dataset.load_indices().await.unwrap();
    assert_eq!(existing.len(), 2);
    assert_eq!(existing[0].fields, existing[1].fields);
    assert_ne!(existing[0].name, existing[1].name);
    assert!(
        existing
            .iter()
            .any(|index| index.name.starts_with("oas_btree_"))
    );
    assert!(
        existing
            .iter()
            .any(|index| index.name.starts_with("oas_text_"))
    );

    options.mode = WriteMode::Append;
    let before = storage.list(&url).await.unwrap();
    assert!(
        sink.open(url.clone(), batch.schema(), options.clone())
            .await
            .is_err()
    );
    assert_eq!(storage.list(&url).await.unwrap(), before);

    options.indexes = indexes
        .into_iter()
        .map(|index| index.with_existing_policy(ExistingIndexPolicy::Keep))
        .collect();
    let mut writer = sink
        .open(url.clone(), batch.schema(), options)
        .await
        .unwrap();
    writer.write(batch).await.unwrap();
    assert_eq!(writer.commit().await.unwrap().rows_written, 3);
    let dataset = sink.dataset_builder(url).unwrap().load().await.unwrap();
    assert_eq!(dataset.count_rows(None).await.unwrap(), 6);
    assert_eq!(dataset.load_indices().await.unwrap().len(), 2);
}
