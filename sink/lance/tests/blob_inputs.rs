use std::sync::Arc;

use arrow_array::{ArrayRef, LargeBinaryArray, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use open_arrow_sink_core::{BLOB_METADATA_KEY, Sink, WriteMode};
use open_arrow_sink_lance::{LanceDatasetSink, Storage};

fn storage() -> Storage {
    open_arrow_sink_test_support::memory_storage("memory:///warehouse/")
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
