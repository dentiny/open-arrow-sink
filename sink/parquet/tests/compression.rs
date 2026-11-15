use std::{collections::BTreeMap, sync::Arc};

use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray, StructArray};
use futures::TryStreamExt;
use open_arrow_sink_core::{
    Compression as Codec, CompressionOptions, Ordering, Sink, SinkExt, WriteOptions,
};
use open_arrow_sink_layer::CompressionLayer;
use open_arrow_sink_parquet::ParquetDirectorySink;
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

#[tokio::test]
async fn compression_survives_rolling_and_round_trips() {
    for (codec, expected_codec) in [
        (Codec::Uncompressed, Compression::UNCOMPRESSED),
        #[cfg(feature = "snappy")]
        (Codec::Snappy, Compression::SNAPPY),
        #[cfg(feature = "gzip")]
        (Codec::Gzip, Compression::GZIP(Default::default())),
        #[cfg(feature = "zstd")]
        (
            Codec::Zstd { level: None },
            Compression::ZSTD(Default::default()),
        ),
        #[cfg(feature = "lz4")]
        (Codec::Lz4Raw, Compression::LZ4_RAW),
    ] {
        let temp = tempfile::tempdir().unwrap();
        let directory = temp.path().join("output");
        let batch = batch();
        let sink = ParquetDirectorySink.layer(CompressionLayer::new(CompressionOptions {
            global: Some(codec),
            columns: BTreeMap::from([("literal.dot".into(), Codec::Uncompressed)]),
        }));
        assert_eq!(sink.capability(), ParquetDirectorySink.capability());
        let mut writer = sink
            .open(
                directory.clone(),
                batch.schema(),
                WriteOptions {
                    ordering: Ordering::Unordered,
                    target_file_size: Some(1),
                    // The layer replaces these session settings.
                    compression: CompressionOptions {
                        global: None,
                        columns: BTreeMap::from([("missing".into(), Codec::Snappy)]),
                    },
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        writer.write(batch.clone()).await.unwrap();
        writer.write(batch.clone()).await.unwrap();
        assert_eq!(writer.commit().await.unwrap().rows_written, 6);
        for index in 0..2 {
            let file = tokio::fs::File::open(directory.join(format!("part-{index:05}.parquet")))
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
}
