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

#[rstest::rstest]
#[case::serial(false)]
#[case::parallel(true)]
#[tokio::test]
async fn compression_survives_rolling_and_round_trips(#[case] parallel: bool) {
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
        let compression = CompressionLayer::new(CompressionOptions {
            global: Some(codec),
            columns: BTreeMap::from([("literal.dot".into(), Codec::Uncompressed)]),
        });
        let sink: Box<dyn Sink<Request = std::path::PathBuf>> = if parallel {
            Box::new(
                ParquetDirectorySink
                    .layer(open_arrow_sink_layer::UnorderedWriteLayer::new(
                        std::num::NonZeroUsize::new(2).unwrap(),
                    ))
                    .layer(compression),
            )
        } else {
            Box::new(ParquetDirectorySink.layer(compression))
        };
        assert_eq!(
            sink.capability().compression,
            ParquetDirectorySink.capability().compression
        );
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
        let mut rows = 0;
        for entry in std::fs::read_dir(&directory).unwrap() {
            let file = tokio::fs::File::open(entry.unwrap().path()).await.unwrap();
            let builder = ParquetRecordBatchStreamBuilder::new(file).await.unwrap();
            if builder.metadata().num_row_groups() == 0 {
                continue;
            }
            let group = builder.metadata().row_group(0);
            assert_eq!(group.column(0).compression(), expected_codec);
            assert_eq!(group.column(1).compression(), Compression::UNCOMPRESSED);
            let actual: Vec<_> = builder.build().unwrap().try_collect().await.unwrap();
            for actual in actual {
                rows += actual.num_rows();
                assert_eq!(actual, batch);
            }
        }
        assert_eq!(rows, 6);
    }
}
