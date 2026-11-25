use arrow_array::{
    Array, ArrayRef, LargeBinaryArray, ListArray, RecordBatch, StringArray, StructArray,
    cast::AsArray,
};
use arrow_buffer::{NullBuffer, OffsetBuffer};
use arrow_schema::{DataType, Field, Schema};
use lance::datatypes::BlobHandling;
use open_arrow_sink_core::{BLOB_METADATA_KEY, Sink, SinkExt, WriteMode};
use open_arrow_sink_lance::LanceDatasetSink;
use open_arrow_sink_layer::DownloadBlobLayer;
use open_arrow_sink_test_support::memory_warehouse as storage;
use open_arrow_sink_test_support::unordered_options as options;
use open_arrow_sink_test_support::url as fixture_url;
use open_arrow_sink_test_support::within;
use std::sync::Arc;

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

#[rstest::rstest]
#[case::create(WriteMode::Create)]
#[case::append(WriteMode::Append)]
#[tokio::test]
async fn writes_to_a_blob_dataset(#[case] mode: WriteMode) {
    let storage = storage();
    let url = fixture_url(&storage, "existing/");
    let downloader = open_arrow_sink_storage::BlobDownloader::new(1.try_into().unwrap()).unwrap();
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
    // Multiple inline payloads can be materialized together without a byte budget.
    within(async {
        writer.write(input()).await.unwrap();
        writer.commit().await.unwrap();
    })
    .await;
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
    let source = open_arrow_sink_test_support::memory_storage("memory://source/");
    let visible = fixture_url(&source, "visible");
    source
        .write(&source.path(&visible).unwrap(), "ok".into())
        .await
        .unwrap();
    let missing = fixture_url(&source, "missing");
    let blobs = StringArray::from(vec![missing.as_str(), missing.as_str(), visible.as_str()]);
    let blob = Arc::new(Field::new("item", DataType::Utf8, true));
    let lists = ListArray::new(
        blob.clone(),
        OffsetBuffer::from_lengths([1, 1, 1]),
        Arc::new(blobs),
        Some(NullBuffer::from(vec![true, false, true])),
    );
    let items = Arc::new(Field::new("items", DataType::List(blob), true));
    let parent = StructArray::new(
        vec![items].into(),
        vec![Arc::new(lists)],
        Some(NullBuffer::from(vec![false, true, true])),
    );
    let batch = RecordBatch::try_from_iter([
        ("parent", Arc::new(parent) as ArrayRef),
        (
            "top",
            Arc::new(StringArray::from(vec![visible.as_str(); 3])),
        ),
        (
            "untouched",
            Arc::new(StringArray::from(vec![missing.as_str(); 3])),
        ),
    ])
    .unwrap();
    let destination = storage();
    let url = fixture_url(&destination, "nested/");
    let sink = LanceDatasetSink::new(destination).with_source_storage(source);
    let mut writer = sink
        .clone()
        .layer(DownloadBlobLayer::new(
            ["parent.items".into(), "top".into()].into(),
        ))
        .open(url.clone(), batch.schema(), options())
        .await
        .unwrap();
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
    assert_eq!(output.column(1).as_binary::<i64>().value(0), b"ok");
    assert_eq!(
        output.column(2).as_string::<i32>().value(0),
        missing.as_str()
    );
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

#[tokio::test]
async fn sliced_nested_blobs_preserve_sibling_columns_across_writes() {
    use arrow_array::{Array, Int32Array, ListArray, StringArray, StructArray};
    use arrow_buffer::OffsetBuffer;
    use lance::blob::{BlobArrayBuilder, blob_field};

    let mut blobs = BlobArrayBuilder::new(3);
    blobs.push_uri("memory://missing/outside-slice").unwrap();
    blobs.push_bytes(b"first").unwrap();
    blobs.push_bytes(b"second").unwrap();
    let blob = Arc::new(blob_field("item", true));
    let lists = ListArray::new(
        blob.clone(),
        OffsetBuffer::from_lengths([1, 1, 1]),
        blobs.finish().unwrap(),
        None,
    );
    let fields = vec![
        Arc::new(Field::new("left", DataType::Int32, false)),
        Arc::new(Field::new("items", DataType::List(blob), true)),
        Arc::new(Field::new("right", DataType::Utf8, true)),
    ];
    let parent = StructArray::new(
        fields.into(),
        vec![
            Arc::new(Int32Array::from(vec![0, 10, 20])),
            Arc::new(lists),
            Arc::new(StringArray::from(vec![Some("outside"), None, Some("kept")])),
        ],
        None,
    );
    let schema = Arc::new(Schema::new(vec![
        Field::new("before", DataType::Int32, false),
        Field::new("parent", parent.data_type().clone(), true),
        Field::new("after", DataType::Int32, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![0, 1, 2])),
            Arc::new(parent),
            Arc::new(Int32Array::from(vec![0, 100, 200])),
        ],
    )
    .unwrap();
    let destination = storage();
    let url = fixture_url(&destination, "sliced/");
    let sink = LanceDatasetSink::new(destination);
    let mut writer = sink.open(url.clone(), schema, options()).await.unwrap();
    for row in 1..3 {
        writer.write(batch.slice(row, 1)).await.unwrap();
    }
    writer.commit().await.unwrap();
    let output = sink
        .dataset_builder(url)
        .unwrap()
        .load()
        .await
        .unwrap()
        .scan()
        .blob_handling(lance::datatypes::BlobHandling::AllBinary)
        .try_into_batch()
        .await
        .unwrap();
    assert_eq!(output.num_rows(), 2);
    let parent = output.column(1).as_struct();
    for row in 0..output.num_rows() {
        let id = output
            .column(0)
            .as_primitive::<arrow_array::types::Int32Type>()
            .value(row);
        assert_eq!(
            parent
                .column(0)
                .as_primitive::<arrow_array::types::Int32Type>()
                .value(row),
            id * 10
        );
        assert_eq!(
            output
                .column(2)
                .as_primitive::<arrow_array::types::Int32Type>()
                .value(row),
            id * 100
        );
        let expected: &[u8] = if id == 1 { b"first" } else { b"second" };
        assert_eq!(
            parent
                .column(1)
                .as_list::<i32>()
                .value(row)
                .as_binary::<i64>()
                .value(0),
            expected
        );
        assert_eq!(
            parent.column(2).as_string::<i32>().iter().nth(row).unwrap(),
            (id == 2).then_some("kept")
        );
    }
}

#[rstest::rstest]
#[case(DataType::Binary)]
#[case(DataType::BinaryView)]
#[tokio::test]
async fn writes_binary_payload_types_without_treating_them_as_urls(#[case] data_type: DataType) {
    let data: ArrayRef = Arc::new(LargeBinaryArray::from(vec![
        Some(b"outside slice".as_slice()),
        None,
        Some(b"".as_slice()),
        Some(b"\x00\xff\xfe".as_slice()),
    ]));
    let data = arrow_cast::cast(&data, &data_type).unwrap().slice(1, 3);
    let field = Field::new("blob", data_type, true)
        .with_metadata([(BLOB_METADATA_KEY.into(), "true".into())].into());
    let batch = RecordBatch::try_new(Arc::new(Schema::new(vec![field])), vec![data]).unwrap();
    let storage = storage();
    let url = fixture_url(&storage, "binary-types/");
    let sink = LanceDatasetSink::new(storage);
    let mut writer = sink
        .open(url.clone(), batch.schema(), options())
        .await
        .unwrap();
    writer.write(batch).await.unwrap();
    writer.commit().await.unwrap();
    let dataset = sink.dataset_builder(url).unwrap().load().await.unwrap();
    let output = dataset
        .scan()
        .blob_handling(BlobHandling::AllBinary)
        .try_into_batch()
        .await
        .unwrap();
    let mut values = output
        .column(0)
        .as_binary::<i64>()
        .iter()
        .map(|v| v.map(<[u8]>::to_vec))
        .collect::<Vec<_>>();
    values.sort();
    assert_eq!(values, vec![None, Some(vec![]), Some(vec![0, 255, 254])]);
}
