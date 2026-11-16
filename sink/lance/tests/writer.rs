use std::sync::Arc;

use arrow_array::{ArrayRef, Int64Array, RecordBatch, cast::AsArray};
use lance::Dataset;
use more_asserts::assert_gt;
use open_arrow_sink_core::{Sink, WriteMode, WriteOptions};
use open_arrow_sink_lance::{LanceDatasetSink, Url};

fn batch() -> RecordBatch {
    open_arrow_sink_test_support::int64_batch("value", vec![Some(3), None, Some(1), Some(3)])
}

use open_arrow_sink_test_support::{
    concat_sorted, seeded_bytes, sort_batch, unordered_options as options,
};

async fn read_sorted(dataset: &Dataset, key: &str) -> RecordBatch {
    sort_batch(&dataset.scan().try_into_batch().await.unwrap(), &[key]).unwrap()
}

struct ConformanceFixture {
    _temp: tempfile::TempDir,
    path: std::path::PathBuf,
    sink: LanceDatasetSink,
}

#[open_arrow_sink_core::async_trait]
impl open_arrow_sink_test_support::conformance::SinkFixture for ConformanceFixture {
    type Sink = LanceDatasetSink;
    async fn new() -> Self {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("dataset");
        Self {
            _temp: temp,
            path,
            sink: LanceDatasetSink::default(),
        }
    }
    fn sink(&self) -> &Self::Sink {
        &self.sink
    }
    fn request(&self) -> Url {
        Url::from_directory_path(&self.path).unwrap()
    }
    fn batch(&self) -> RecordBatch {
        batch()
    }
    fn options(&self) -> WriteOptions {
        options()
    }
    async fn read(&self) -> Vec<RecordBatch> {
        let dataset = Dataset::open(self.path.to_str().unwrap()).await.unwrap();
        vec![read_sorted(&dataset, "value").await]
    }
    async fn assert_aborted(&self) {
        assert!(!tokio::fs::try_exists(&self.path).await.unwrap());
    }
}

#[tokio::test]
async fn shared_sink_contracts() {
    open_arrow_sink_test_support::conformance::assert_conforms::<ConformanceFixture>().await;
}

#[tokio::test]
async fn abort_removes_dataset_and_preserves_sibling_files() {
    let temp = tempfile::tempdir().unwrap();
    let sibling = temp.path().join("keep.txt");
    tokio::fs::write(&sibling, "keep").await.unwrap();
    let path = temp.path().join("dataset");
    let batch = batch();
    let mut writer = LanceDatasetSink::default()
        .open(
            Url::from_directory_path(&path).unwrap(),
            batch.schema(),
            options(),
        )
        .await
        .unwrap();
    writer.write(batch).await.unwrap();
    writer.abort().await.unwrap();
    assert!(!tokio::fs::try_exists(&path).await.unwrap());
    assert_eq!(tokio::fs::read_to_string(sibling).await.unwrap(), "keep");
}

#[tokio::test]
async fn target_file_size_rolls_files_without_losing_rows() {
    // Exceed the SDK's default page buffer while staying below its row-count limit.
    let count = 10 * 1024;
    let payloads = arrow_array::FixedSizeBinaryArray::try_from_iter(
        (0..count).map(|id| seeded_bytes(id as u64 + 1, 1024)),
    )
    .unwrap();
    let expected = RecordBatch::try_from_iter([
        (
            "id",
            Arc::new(Int64Array::from_iter_values(0..count as i64)) as ArrayRef,
        ),
        ("payload", Arc::new(payloads) as ArrayRef),
    ])
    .unwrap();
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("dataset");
    let mut writer = LanceDatasetSink::default()
        .open(
            Url::from_directory_path(std::path::absolute(&path).unwrap()).unwrap(),
            expected.schema(),
            options().with_target_file_size(Some(1)),
        )
        .await
        .unwrap();
    writer.write(expected.clone()).await.unwrap();
    writer.write(expected.clone()).await.unwrap();
    assert_eq!(
        writer.commit().await.unwrap().rows_written,
        2 * count as u64
    );
    let dataset = Dataset::open(path.to_str().unwrap()).await.unwrap();
    let fragments = dataset.get_fragments();
    assert_gt!(fragments.len(), 1);
    let actual = read_sorted(&dataset, "id").await;
    let expected =
        concat_sorted(&expected.schema(), &[expected.clone(), expected], &["id"]).unwrap();
    assert_eq!(actual, expected);
}

#[tokio::test]
async fn compression_survives_rolling_and_round_trips() {
    use std::collections::HashMap;

    use arrow_array::{FixedSizeBinaryArray, StringArray, StructArray};
    use arrow_schema::{DataType, Field, Schema};
    use open_arrow_sink_core::{Compression, CompressionOptions, SinkExt};
    use open_arrow_sink_layer::CompressionLayer;

    let count = 10 * 1024;
    let text = StringArray::from_iter_values(
        (0..count).map(|id| format!("{id:08}:{}", "compressible payload ".repeat(100))),
    );
    let raw =
        FixedSizeBinaryArray::try_from_iter((0..count).map(|id| seeded_bytes(id as u64 + 1, 1024)))
            .unwrap();
    let text_field = Arc::new(Field::new("text", DataType::Utf8, false).with_metadata(
        HashMap::from([
            ("custom".into(), "preserved".into()),
            ("lance-encoding:compression".into(), "none".into()),
            ("lance-encoding:compression-level".into(), "17".into()),
        ]),
    ));
    let nested = StructArray::new(vec![text_field].into(), vec![Arc::new(text.clone())], None);
    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from_iter_values(0..count as i64)),
        Arc::new(nested),
        Arc::new(raw.clone()),
    ];
    let schema = Arc::new(Schema::new_with_metadata(
        vec![
            Field::new("id", columns[0].data_type().clone(), false),
            Field::new("nested", columns[1].data_type().clone(), false),
            Field::new("raw_payload", columns[2].data_type().clone(), false),
        ],
        HashMap::from([("application".into(), "preserved".into())]),
    ));
    let expected = RecordBatch::try_new(schema.clone(), columns).unwrap();
    let sink = LanceDatasetSink::default().layer(CompressionLayer::new(CompressionOptions {
        global: Some(Compression::Zstd { level: Some(9) }),
        columns: [
            ("nested".into(), Compression::Zstd { level: Some(3) }),
            ("raw_payload".into(), Compression::Uncompressed),
        ]
        .into(),
        ..Default::default()
    }));
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("dataset");
    let mut writer = sink
        .open(
            Url::from_directory_path(std::path::absolute(&path).unwrap()).unwrap(),
            schema.clone(),
            options().with_target_file_size(Some(1)),
        )
        .await
        .unwrap();
    assert_eq!(writer.schema(), &schema);
    writer.write(expected.clone()).await.unwrap();
    writer.write(expected.clone()).await.unwrap();
    assert_eq!(
        writer.commit().await.unwrap().rows_written,
        2 * count as u64
    );

    let dataset = Dataset::open(path.to_str().unwrap()).await.unwrap();
    assert_gt!(dataset.get_fragments().len(), 1);
    let actual = dataset.scan().try_into_batch().await.unwrap();
    let stored_schema = actual.schema();
    assert_eq!(stored_schema.metadata(), schema.metadata());
    assert_eq!(
        stored_schema.field(0).metadata()["lance-encoding:compression"],
        "zstd"
    );
    assert_eq!(
        stored_schema.field(0).metadata()["lance-encoding:compression-level"],
        "9"
    );
    let DataType::Struct(children) = stored_schema.field(1).data_type() else {
        panic!("expected nested struct");
    };
    assert_eq!(children[0].metadata()["custom"], "preserved");
    assert_eq!(children[0].metadata()["lance-encoding:compression"], "zstd");
    assert_eq!(
        children[0].metadata()["lance-encoding:compression-level"],
        "3"
    );
    assert_eq!(
        stored_schema.field(2).metadata()["lance-encoding:compression"],
        "none"
    );
    assert!(
        !stored_schema
            .field(2)
            .metadata()
            .contains_key("lance-encoding:compression-level")
    );
    // The original schema and its existing metadata are still the caller's contract.
    let DataType::Struct(children) = schema.field(1).data_type() else {
        panic!("expected nested struct");
    };
    assert_eq!(children[0].metadata()["lance-encoding:compression"], "none");
    assert_eq!(
        children[0].metadata()["lance-encoding:compression-level"],
        "17"
    );

    // Encoding metadata intentionally differs. Compare every value separately
    // from the metadata assertions above, retaining both copies of every row.
    fn values(batch: &RecordBatch) -> RecordBatch {
        RecordBatch::try_from_iter([
            ("id", batch.column(0).clone()),
            ("text", batch.column(1).as_struct().column(0).clone()),
            ("raw_payload", batch.column(2).clone()),
        ])
        .unwrap()
    }
    let actual_values = sort_batch(&values(&actual), &["id"]).unwrap();
    let expected_values = values(&expected);
    let expected_values = concat_sorted(
        &expected_values.schema(),
        &[expected_values.clone(), expected_values],
        &["id"],
    )
    .unwrap();
    assert_eq!(actual_values, expected_values);
}

#[rstest::rstest]
#[case::append(WriteMode::Append, 5)]
#[case::overwrite(WriteMode::Overwrite, 4)]
#[tokio::test]
async fn write_mode(#[case] mode: WriteMode, #[case] expected_rows: usize) {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("dataset");
    let batch = batch();
    let mut writer = LanceDatasetSink::default()
        .open(
            Url::from_directory_path(&path).unwrap(),
            batch.schema(),
            options(),
        )
        .await
        .unwrap();
    writer.write(batch.slice(0, 1)).await.unwrap();
    writer.commit().await.unwrap();
    let mut writer = LanceDatasetSink::default()
        .open(
            Url::from_directory_path(&path).unwrap(),
            batch.schema(),
            options().with_mode(mode),
        )
        .await
        .unwrap();
    writer.write(batch).await.unwrap();
    writer.commit().await.unwrap();
    let dataset = Dataset::open(path.to_str().unwrap()).await.unwrap();
    assert_eq!(dataset.count_rows(None).await.unwrap(), expected_rows);
}
