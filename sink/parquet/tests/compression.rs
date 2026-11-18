use std::{collections::BTreeMap, sync::Arc};

use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray, StructArray};
use futures::TryStreamExt;
use open_arrow_sink_core::{
    CompressionOptions, CompressionSpec, Ordering, Sink, SinkExt, WriteOptions,
};
use open_arrow_sink_layer::CompressionLayer;
use open_arrow_sink_parquet::{ParquetDirectorySink, Url};
use parquet::{arrow::ParquetRecordBatchStreamBuilder, basic::Compression};

fn batch() -> RecordBatch {
    let nested = StructArray::from(
        RecordBatch::try_from_iter([(
            "value",
            Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef,
        )])
        .unwrap(),
    );
    RecordBatch::try_from_iter([
        ("nested", Arc::new(nested) as ArrayRef),
        (
            "literal.dot",
            Arc::new(StringArray::from(vec!["a", "b", "c"])) as ArrayRef,
        ),
    ])
    .unwrap()
}

#[rstest::rstest]
#[case::uncompressed(CompressionSpec::Uncompressed, Compression::UNCOMPRESSED)]
#[cfg_attr(
    feature = "snappy",
    case::snappy(CompressionSpec::Snappy, Compression::SNAPPY)
)]
#[cfg_attr(
    feature = "gzip",
    case::gzip(CompressionSpec::Gzip, Compression::GZIP(Default::default()))
)]
#[cfg_attr(feature = "zstd", case::zstd(CompressionSpec::Zstd { level: None }, Compression::ZSTD(Default::default())))]
#[cfg_attr(
    feature = "lz4",
    case::lz4(CompressionSpec::Lz4Raw, Compression::LZ4_RAW)
)]
#[tokio::test]
async fn compression_survives_rolling_and_round_trips(
    #[case] codec: CompressionSpec,
    #[case] expected_codec: Compression,
) {
    let temp = tempfile::tempdir().unwrap();
    let directory = temp.path().join("output");
    let batch = batch();
    let sink = ParquetDirectorySink::default().layer(CompressionLayer::new(CompressionOptions {
        global: Some(codec),
        columns: BTreeMap::from([("literal.dot".into(), CompressionSpec::Uncompressed)]),
        ..Default::default()
    }));
    assert_eq!(
        sink.capability(),
        ParquetDirectorySink::default().capability()
    );
    let mut writer = sink
        .open(
            Url::from_directory_path(&directory).unwrap(),
            batch.schema(),
            open_arrow_sink_test_support::unordered_options()
                .with_target_file_size(1)
                .with_compression(CompressionOptions {
                    global: None,
                    columns: BTreeMap::from([("missing".into(), CompressionSpec::Snappy)]),
                    ..Default::default()
                }),
        )
        .await
        .unwrap();
    writer.write(batch.clone()).await.unwrap();
    writer.write(batch.clone()).await.unwrap();
    writer.commit().await.unwrap();
    for index in 0..2 {
        let file = tokio::fs::File::open(directory.join(format!("part-0-{index:05}.parquet")))
            .await
            .unwrap();
        let builder = ParquetRecordBatchStreamBuilder::new(file).await.unwrap();
        let group = builder.metadata().row_group(0);
        assert_eq!(group.column(0).compression(), expected_codec);
        assert_eq!(group.column(1).compression(), Compression::UNCOMPRESSED);
        let actual: Vec<_> = builder.build().unwrap().try_collect().await.unwrap();
        assert_eq!(actual, vec![batch.clone()]);
    }
}

#[tokio::test]
async fn option_layers_compose_before_backend_validation() {
    use open_arrow_sink_layer::{OrderingLayer, ParallelIngestionLayer};
    use std::num::NonZeroUsize;

    // The compression layer replaces an invalid caller policy. Ordering is
    // enabled by a layer so callers can retain WriteOptions' default ordering.
    let compression = CompressionLayer::new(CompressionOptions {
        global: Some(CompressionSpec::Uncompressed),
        ..Default::default()
    });
    let ordering = OrderingLayer::new(Ordering::Unordered);
    let unordered = ParallelIngestionLayer::fixed(NonZeroUsize::new(2).unwrap());
    let sinks: Vec<Box<dyn Sink<Request = Url>>> = vec![
        Box::new(
            ParquetDirectorySink::default()
                .layer(ordering)
                .layer(compression.clone()),
        ),
        Box::new(
            ParquetDirectorySink::default()
                .layer(compression.clone())
                .layer(ordering),
        ),
        Box::new(
            ParquetDirectorySink::default()
                .layer(compression.clone())
                .layer(unordered)
                .layer(ordering),
        ),
        Box::new(
            ParquetDirectorySink::default()
                .layer(unordered)
                .layer(ordering)
                .layer(compression.clone()),
        ),
        Box::new(
            ParquetDirectorySink::default()
                .layer(unordered)
                .layer(compression)
                .layer(ordering),
        ),
    ];
    for sink in sinks {
        let temp = tempfile::tempdir().unwrap();
        let directory = temp.path().join("output");
        let batch = batch();
        let options = WriteOptions::default().with_compression(CompressionOptions {
            columns: [("missing".into(), CompressionSpec::Snappy)].into(),
            ..Default::default()
        });
        // Deferring validation must still reject invalid effective options
        // before the backend creates a destination.
        assert!(
            sink.open(
                Url::from_directory_path(&directory).unwrap(),
                batch.schema(),
                options.clone().with_target_file_size(0),
            )
            .await
            .is_err()
        );
        assert!(!directory.exists());
        let mut writer = sink
            .open(
                Url::from_directory_path(&directory).unwrap(),
                batch.schema(),
                options,
            )
            .await
            .unwrap();
        writer.write(batch).await.unwrap();
        writer.commit().await.unwrap();
    }
}
