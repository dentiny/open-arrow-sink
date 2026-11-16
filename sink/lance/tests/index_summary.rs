use std::{num::NonZeroUsize, sync::Arc};

use arrow_array::{ArrayRef, Int64Array, LargeBinaryArray, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use open_arrow_sink_core::{
    BLOB_METADATA_KEY, Error, IndexCommitError, IndexSpec, IndexType, Ordering, Sink, SinkExt,
    WriteOptions,
};
use open_arrow_sink_lance::{LanceDatasetSink, Url};
use open_arrow_sink_layer::UnorderedWriteLayer;

#[rstest::rstest]
#[case::serial(false, false)]
#[case::parallel(true, false)]
#[case::blob(false, true)]
#[tokio::test]
async fn index_failure_retains_committed_summary(#[case] parallel: bool, #[case] blob: bool) {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("dataset");
    let mut fields = vec![Field::new("value", DataType::Int64, false)];
    let mut columns: Vec<ArrayRef> = vec![Arc::new(Int64Array::from(vec![1, 2, 3]))];
    if blob {
        fields.push(
            Field::new("blob", DataType::LargeBinary, true)
                .with_metadata([(BLOB_METADATA_KEY.into(), "true".into())].into()),
        );
        columns.push(Arc::new(LargeBinaryArray::from(vec![
            Some(
                b"payload".as_slice()
            );
            3
        ])));
    }
    let input = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap();
    let mut options = WriteOptions::default();
    options.ordering = Ordering::Unordered;
    options.indexes = vec![IndexSpec::new("value", IndexType::Scalar)];
    let sink = LanceDatasetSink::default();
    let url = Url::from_directory_path(&path).unwrap();
    let mut writer = if parallel {
        sink.layer(UnorderedWriteLayer::new(NonZeroUsize::new(2).unwrap()))
            .open(url, input.schema(), options)
            .await
    } else {
        sink.open(url, input.schema(), options).await
    }
    .unwrap();
    writer.write(input.clone()).await.unwrap();
    // Data publication writes _versions; only index construction needs this
    // directory. A regular file deterministically prevents index object writes.
    tokio::fs::write(path.join("_indices"), b"block index writes")
        .await
        .unwrap();
    let error = writer.commit().await.unwrap_err();
    let Error::Backend(source) = error else {
        panic!("expected backend index error")
    };
    let context = source
        .downcast_ref::<IndexCommitError>()
        .expect("typed index context");
    assert_eq!(context.summary.rows_written, input.num_rows() as u64);
    assert!(context.message.contains("Scalar"));
    let dataset = lance::Dataset::open(path.to_str().unwrap()).await.unwrap();
    assert_eq!(dataset.count_rows(None).await.unwrap(), input.num_rows());
    assert_eq!(
        dataset.scan().try_into_batch().await.unwrap().num_rows(),
        input.num_rows()
    );
}
