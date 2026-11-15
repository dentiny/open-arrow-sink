use arrow_array::{
    Array, ArrayRef, BinaryArray, Int64Array, LargeBinaryArray, ListArray, RecordBatch,
    StructArray,
    builder::{BinaryBuilder, ListBuilder, StructBuilder},
};
use arrow_schema::{DataType, Field};
use open_arrow_sink_core::{Ordering, Sink, SinkExt, WriteMode, WriteOptions};
use open_arrow_sink_lance::{AtifStorageLayer, LanceDatasetSink, Url};
use open_arrow_sink_layer::UnorderedWriteLayer;
use std::{num::NonZeroUsize, sync::Arc};

fn marked(name: &str, policy: &str) -> Field {
    Field::new(name, DataType::Binary, true).with_metadata(
        [
            ("lance_conversion:source_type".into(), "atif".into()),
            ("lance_conversion:storage_policy".into(), policy.into()),
            ("custom".into(), "kept".into()),
        ]
        .into(),
    )
}
fn input() -> RecordBatch {
    let mut list = ListBuilder::new(StructBuilder::from_fields(
        vec![
            marked("media", "blob"),
            marked("text", "text"),
            Field::new("raw", DataType::Binary, true),
        ],
        4,
    ));
    for (media, text, raw, valid) in [
        (Some(b"discard".as_slice()), None, None, true),
        (
            Some(b"image".as_slice()),
            None,
            Some(b"raw".as_slice()),
            true,
        ),
        (None, Some(b"hello".as_slice()), None, true),
        (None, None, None, false),
    ] {
        list.values()
            .field_builder::<BinaryBuilder>(0)
            .unwrap()
            .append_option(media);
        list.values()
            .field_builder::<BinaryBuilder>(1)
            .unwrap()
            .append_option(text);
        list.values()
            .field_builder::<BinaryBuilder>(2)
            .unwrap()
            .append_option(raw);
        list.values().append(valid);
        if media == Some(b"discard".as_slice()) {
            list.append(true);
        }
        if text.is_some() {
            list.append(true);
            list.append(false);
            list.append(true);
        }
    }
    list.append(true);
    RecordBatch::try_from_iter([
        (
            "id",
            Arc::new(Int64Array::from(vec![99, 0, 1, 2, 3])) as ArrayRef,
        ),
        ("parts", Arc::new(list.finish()) as ArrayRef),
    ])
    .unwrap()
    .slice(1, 4)
}
fn options(mode: WriteMode) -> WriteOptions {
    WriteOptions {
        mode,
        blob_handle: Some(open_arrow_sink_core::BlobHandleConfig {
            inline_size_threshold: Some(1),
            dedicated_size_threshold: NonZeroUsize::new(2),
            ..Default::default()
        }),
        ordering: Ordering::Unordered,
        ..Default::default()
    }
}

#[rstest::rstest]
#[case::single(false)]
#[case::parallel(true)]
#[tokio::test]
async fn nested_media_round_trips_and_appends(#[case] parallel: bool) {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("dataset");
    let url = Url::from_directory_path(&path).unwrap();
    let sink = LanceDatasetSink::default().layer(AtifStorageLayer);
    let sink: Box<dyn Sink<Request = Url>> = if parallel {
        Box::new(sink.layer(UnorderedWriteLayer::new(NonZeroUsize::new(2).unwrap())))
    } else {
        Box::new(sink)
    };
    let input = input();
    for mode in [WriteMode::Create, WriteMode::Append] {
        let mut writer = sink
            .open(url.clone(), input.schema(), options(mode))
            .await
            .unwrap();
        assert_eq!(writer.schema(), &input.schema());
        writer.write(input.clone()).await.unwrap();
        assert_eq!(writer.commit().await.unwrap().rows_written, 4);
    }
    let dataset = lance::Dataset::open(path.to_str().unwrap()).await.unwrap();
    let actual = dataset
        .scan()
        .blob_handling(lance::datatypes::BlobHandling::AllBinary)
        .try_into_batch()
        .await
        .unwrap();
    assert_eq!(actual.num_rows(), 8);
    let ids = actual
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let parts = actual
        .column(1)
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    for row in 0..actual.num_rows() {
        match ids.value(row) {
            0 => {
                let items = parts.value(row);
                let items = items.as_any().downcast_ref::<StructArray>().unwrap();
                let media = items
                    .column(0)
                    .as_any()
                    .downcast_ref::<LargeBinaryArray>()
                    .unwrap();
                let text = items
                    .column(1)
                    .as_any()
                    .downcast_ref::<LargeBinaryArray>()
                    .unwrap();
                assert_eq!(media.value(0), b"image");
                assert!(media.is_null(1));
                assert!(text.is_null(0));
                assert_eq!(text.value(1), b"hello");
                assert_eq!(
                    items
                        .column(2)
                        .as_any()
                        .downcast_ref::<BinaryArray>()
                        .unwrap()
                        .value(0),
                    b"raw"
                );
                assert_eq!(items.fields()[1].metadata()["custom"], "kept");
                assert_eq!(
                    items.fields()[1].metadata()["lance-encoding:compression"],
                    "zstd"
                );
            }
            1 => assert!(parts.is_null(row)),
            2 => assert_eq!(parts.value_length(row), 0),
            3 => {
                let items = parts.value(row);
                assert!(items.is_null(0));
            }
            _ => unreachable!(),
        }
    }
}

#[tokio::test]
async fn invalid_policy_fails_before_destination_creation() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("dataset");
    let schema = Arc::new(arrow_schema::Schema::new(vec![marked("media", "unknown")]));
    assert!(
        LanceDatasetSink::default()
            .layer(AtifStorageLayer)
            .open(
                Url::from_directory_path(&path).unwrap(),
                schema,
                options(WriteMode::Create)
            )
            .await
            .is_err()
    );
    assert!(!path.exists());
}

#[tokio::test]
async fn adaptation_is_opt_in_and_failed_write_aborts() {
    let temp = tempfile::tempdir().unwrap();
    let input = input();
    let raw = temp.path().join("raw");
    let mut writer = LanceDatasetSink::default()
        .open(
            Url::from_directory_path(&raw).unwrap(),
            input.schema(),
            options(WriteMode::Create),
        )
        .await
        .unwrap();
    writer.write(input.clone()).await.unwrap();
    writer.commit().await.unwrap();
    let dataset = lance::Dataset::open(raw.to_str().unwrap()).await.unwrap();
    let raw_batch = dataset.scan().try_into_batch().await.unwrap();
    let lists = raw_batch
        .column(1)
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    let values = lists
        .values()
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    assert_eq!(values.column(0).data_type(), &DataType::Binary);

    let aborted = temp.path().join("aborted");
    let mut writer = LanceDatasetSink::default()
        .layer(AtifStorageLayer)
        .open(
            Url::from_directory_path(&aborted).unwrap(),
            input.schema(),
            options(WriteMode::Create),
        )
        .await
        .unwrap();
    let wrong =
        RecordBatch::try_from_iter([("wrong", Arc::new(Int64Array::from(vec![1])) as ArrayRef)])
            .unwrap();
    assert!(writer.write(wrong).await.is_err());
    assert!(writer.commit().await.is_err());
    assert!(!aborted.exists());
}
