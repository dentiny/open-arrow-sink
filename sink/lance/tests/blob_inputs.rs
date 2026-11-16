use open_arrow_sink_storage::ObjectPath;
use std::sync::Arc;

use arrow_array::{ArrayRef, LargeBinaryArray, RecordBatch, cast::AsArray};
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
            Some(b"first".as_slice()),
            None,
            Some(b"last".as_slice()),
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
    let url = storage
        .url(&ObjectPath::parse("existing/").unwrap())
        .unwrap();
    let downloader =
        open_arrow_sink_storage::BlobDownloader::new(1.try_into().unwrap(), 25.try_into().unwrap())
            .unwrap()
            .with_download_limits(8.try_into().unwrap(), 1.try_into().unwrap())
            .unwrap();
    let sink = LanceDatasetSink::new(storage).with_blob_downloader(Arc::new(downloader));
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
    // The combined payload is larger than one reservation; batch admission must split it.
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        writer.write(input()).await.unwrap();
        writer.commit().await.unwrap();
    })
    .await
    .unwrap();
    let dataset = sink.dataset_builder(url).unwrap().load().await.unwrap();
    let output = dataset
        .scan()
        .blob_handling(lance::datatypes::BlobHandling::AllBinary)
        .try_into_batch()
        .await
        .unwrap();
    let mut values = output
        .column(0)
        .as_binary::<i64>()
        .iter()
        .map(|bytes| bytes.map(<[u8]>::to_vec))
        .collect::<Vec<_>>();
    values.sort();
    assert_eq!(
        values,
        vec![None, Some(b"first".to_vec()), Some(b"last".to_vec())]
    );
}

#[tokio::test]
async fn null_ancestors_do_not_download_hidden_blob_uris() {
    use arrow_array::{Array, ListArray, StructArray};
    use arrow_buffer::{NullBuffer, OffsetBuffer};
    use lance::{
        blob::{BlobArrayBuilder, blob_field},
        datatypes::BlobHandling,
    };
    let source = open_arrow_sink_test_support::memory_storage("memory://source/");
    let visible = source.url(&ObjectPath::parse("visible").unwrap()).unwrap();
    source
        .write(&source.path(&visible).unwrap(), "ok".into())
        .await
        .unwrap();
    let missing = source.url(&ObjectPath::parse("missing").unwrap()).unwrap();
    let mut blobs = BlobArrayBuilder::new(3);
    for uri in [&missing, &missing, &visible] {
        blobs.push_uri(uri.as_str()).unwrap();
    }
    let blob = Arc::new(blob_field("item", true));
    let lists = ListArray::new(
        blob.clone(),
        OffsetBuffer::from_lengths([1, 1, 1]),
        blobs.finish().unwrap(),
        Some(NullBuffer::from(vec![true, false, true])),
    );
    let items = Arc::new(Field::new("items", DataType::List(blob), true));
    let parent = StructArray::new(
        vec![items].into(),
        vec![Arc::new(lists)],
        Some(NullBuffer::from(vec![false, true, true])),
    );
    let schema = Arc::new(Schema::new(vec![Field::new(
        "parent",
        parent.data_type().clone(),
        true,
    )]));
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(parent)]).unwrap();
    let destination = storage();
    let url = destination
        .url(&ObjectPath::parse("nested/").unwrap())
        .unwrap();
    let sink = LanceDatasetSink::new(destination).with_source_storage(source);
    let mut writer = sink.open(url.clone(), schema, options()).await.unwrap();
    writer.write(batch).await.unwrap();
    writer.commit().await.unwrap();
    let output = sink
        .dataset_builder(url)
        .unwrap()
        .load()
        .await
        .unwrap()
        .scan()
        .blob_handling(BlobHandling::AllBinary)
        .try_into_batch()
        .await
        .unwrap();
    let parent = output.column(0).as_struct();
    // Find by null structure since this sink provides no scan ordering guarantee.
    let lists = parent.column(0).as_list::<i32>();
    assert_eq!(parent.null_count(), 1);
    let visible = (0..parent.len())
        .filter(|&row| parent.is_valid(row))
        .collect::<Vec<_>>();
    assert_eq!(visible.iter().filter(|&&row| lists.is_null(row)).count(), 1);
    let row = visible
        .into_iter()
        .find(|&row| lists.is_valid(row))
        .unwrap();
    let bytes = lists.value(row);
    assert_eq!(bytes.as_binary::<i64>().value(0), b"ok");
}
