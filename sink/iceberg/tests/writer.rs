use arrow_array::{
    ArrayRef, Date32Array, Int32Array, Int64Array, RecordBatch, TimestampMicrosecondArray,
};
use futures::TryStreamExt;
use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
use iceberg::spec::{
    Literal, NestedField, PrimitiveType, Schema, Struct, Transform, Type, UnboundPartitionSpec,
};
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableCreation, TableIdent};
use open_arrow_sink_core::{PartitionOptions, PartitionType, SchemaRef, Sink, WriteOptions};
use open_arrow_sink_iceberg::{IcebergTableSink, OpenDalStorageFactory, iceberg};
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

struct TableSpec {
    partition: Option<UnboundPartitionSpec>,
    value_type: PrimitiveType,
    properties: HashMap<String, String>,
}
impl Default for TableSpec {
    fn default() -> Self {
        Self {
            partition: None,
            value_type: PrimitiveType::Long,
            properties: HashMap::new(),
        }
    }
}
struct TestTable {
    catalog: Arc<dyn Catalog>,
    identifier: TableIdent,
    schema: SchemaRef,
}

impl TestTable {
    async fn new(spec: TableSpec) -> Self {
        let catalog: Arc<dyn Catalog> = Arc::new(
            MemoryCatalogBuilder::default()
                .with_storage_factory(Arc::new(OpenDalStorageFactory::new(
                    open_arrow_sink_test_support::memory_storage("memory:///"),
                )))
                .load(
                    "test",
                    HashMap::from([(
                        MEMORY_CATALOG_WAREHOUSE.into(),
                        "memory:///warehouse".into(),
                    )]),
                )
                .await
                .unwrap(),
        );
        let namespace = NamespaceIdent::new("tests".into());
        catalog
            .create_namespace(&namespace, HashMap::new())
            .await
            .unwrap();
        let schema = Schema::builder()
            .with_fields(vec![
                Arc::new(NestedField::optional(
                    1,
                    "id",
                    Type::Primitive(PrimitiveType::Int),
                )),
                Arc::new(NestedField::optional(
                    2,
                    "value",
                    Type::Primitive(spec.value_type),
                )),
            ])
            .build()
            .unwrap();
        let table = catalog
            .create_table(
                &namespace,
                TableCreation::builder()
                    .name("numbers".into())
                    .schema(schema)
                    .partition_spec_opt(spec.partition)
                    .properties(spec.properties)
                    .build(),
            )
            .await
            .unwrap();
        let schema = Arc::new(
            iceberg::arrow::schema_to_arrow_schema(table.metadata().current_schema()).unwrap(),
        );
        Self {
            catalog,
            identifier: TableIdent::new(namespace, "numbers".into()),
            schema,
        }
    }

    fn sink(&self) -> IcebergTableSink {
        IcebergTableSink::new(self.catalog.clone())
    }

    async fn open(&self, options: WriteOptions) -> Box<dyn open_arrow_sink_core::Writer> {
        self.sink()
            .open(self.identifier.clone(), self.schema.clone(), options)
            .await
            .unwrap()
    }

    async fn load(&self) -> iceberg::table::Table {
        self.catalog.load_table(&self.identifier).await.unwrap()
    }

    async fn snapshot(&self) -> Option<i64> {
        self.load().await.metadata().current_snapshot_id()
    }

    async fn rows(&self) -> RecordBatch {
        open_arrow_sink_test_support::concat_sorted(
            &self.schema,
            &read(&self.load().await).await,
            &["id", "value"],
        )
        .unwrap()
    }

    async fn assert_rows(&self, expected: &[RecordBatch]) {
        let actual = self.rows().await;
        let expected = open_arrow_sink_test_support::concat_sorted(
            &actual.schema(),
            expected,
            &["id", "value"],
        )
        .unwrap();
        assert_eq!(actual, expected);
    }

    async fn files(&self) -> Vec<iceberg::scan::FileScanTask> {
        files(&self.load().await).await
    }
}

fn batch(schema: SchemaRef) -> RecordBatch {
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![Some(3), None, Some(1)])),
            Arc::new(Int64Array::from(vec![Some(30), Some(20), None])),
        ],
    )
    .unwrap()
}

fn options() -> WriteOptions {
    open_arrow_sink_test_support::unordered_options()
        .with_mode(open_arrow_sink_core::WriteMode::Append)
}

async fn read(table: &iceberg::table::Table) -> Vec<RecordBatch> {
    table
        .scan()
        .build()
        .unwrap()
        .to_arrow()
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap()
}
async fn files(table: &iceberg::table::Table) -> Vec<iceberg::scan::FileScanTask> {
    table
        .scan()
        .build()
        .unwrap()
        .plan_files()
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap()
}
async fn assert_partitions(table: &iceberg::table::Table, expected: HashSet<Struct>) {
    let files = files(table).await;
    assert_eq!(files.len(), expected.len());
    assert_eq!(
        files
            .iter()
            .map(|task| task.partition().unwrap().clone())
            .collect::<HashSet<_>>(),
        expected
    );
    for task in files {
        assert_eq!(
            task.partition_spec().unwrap().as_ref(),
            table.metadata().default_partition_spec().as_ref()
        );
    }
}

async fn partitioned_round_trip(
    table: &TestTable,
    data: &RecordBatch,
    partition_type: PartitionType,
    slices: &[(usize, usize)],
    expected_partitions: HashSet<Struct>,
) {
    let mut writer = table
        .open(options().with_partition(PartitionOptions { partition_type }))
        .await;
    let mut expected_rows = Vec::new();
    for &(offset, length) in slices {
        let batch = data.slice(offset, length);
        writer.write(batch.clone()).await.unwrap();
        expected_rows.push(batch);
    }
    assert!(table.snapshot().await.is_none());
    writer.commit().await.unwrap();
    table.assert_rows(&expected_rows).await;
    assert_partitions(&table.load().await, expected_partitions).await;
}

struct ConformanceFixture {
    table: TestTable,
    sink: IcebergTableSink,
}

#[open_arrow_sink_core::async_trait]
impl open_arrow_sink_test_support::conformance::SinkFixture for ConformanceFixture {
    type Sink = IcebergTableSink;
    async fn new() -> Self {
        let table = TestTable::new(TableSpec::default()).await;
        Self {
            sink: table.sink(),
            table,
        }
    }
    fn sink(&self) -> &Self::Sink {
        &self.sink
    }
    fn request(&self) -> TableIdent {
        self.table.identifier.clone()
    }
    fn batch(&self) -> RecordBatch {
        batch(self.table.schema.clone())
    }
    fn options(&self) -> WriteOptions {
        options()
    }
    async fn read(&self) -> Vec<RecordBatch> {
        let table = self.table.load().await;
        read(&table).await
    }
    async fn assert_rolled(&self) {
        assert!(self.table.files().await.len() > 1);
    }
    async fn create_sibling(&self) {
        let table = self.table.load().await;
        let mut writer = table
            .file_io()
            .new_output("memory:///warehouse/keep")
            .unwrap()
            .writer()
            .await
            .unwrap();
        writer.write("keep".into()).await.unwrap();
        writer.close().await.unwrap();
    }
    async fn assert_sibling(&self) {
        let table = self.table.load().await;
        assert_eq!(
            table
                .file_io()
                .new_input("memory:///warehouse/keep")
                .unwrap()
                .read()
                .await
                .unwrap(),
            "keep"
        );
    }
    async fn assert_aborted(&self) {
        assert_eq!(self.table.snapshot().await, None);
        assert_eq!(self.table.rows().await.num_rows(), 0);
    }
}

open_arrow_sink_test_support::conformance_tests!(ConformanceFixture);

#[tokio::test]
async fn commits_round_trip_batches_and_preserve_existing_rows() {
    let table = TestTable::new(TableSpec::default()).await;
    let batch = batch(table.schema.clone());
    let mut writer = table.open(options()).await;
    assert_eq!(writer.schema(), &table.schema);
    writer.write(batch.slice(0, 1)).await.unwrap();
    writer.write(batch.slice(1, 2)).await.unwrap();
    let summary = writer.commit().await.unwrap();
    assert_eq!(summary.rows_committed, 3);

    let loaded = table.load().await;
    let data_prefix = format!(
        "{}/data/",
        loaded.metadata().location().trim_end_matches('/')
    );
    let tasks = files(&loaded).await;
    assert!(!tasks.is_empty());
    for task in tasks {
        let name = task.data_file_path().strip_prefix(&data_prefix).unwrap();
        assert!(
            !name.contains('/'),
            "expected a file directly under data/: {name}"
        );
    }
    table.assert_rows(std::slice::from_ref(&batch)).await;

    let mut writer = table.open(options()).await;
    writer.write(batch.slice(0, 1)).await.unwrap();
    let summary = writer.commit().await.unwrap();
    assert_eq!(summary.rows_committed, 1);
    table.assert_rows(&[batch.clone(), batch.slice(0, 1)]).await;
}

#[tokio::test]
async fn abort_preserves_snapshot_and_committed_content() {
    let table = TestTable::new(TableSpec::default()).await;
    let batch = batch(table.schema.clone());
    let mut writer = table.open(options()).await;
    writer.write(batch.clone()).await.unwrap();
    writer.commit().await.unwrap();
    let before = table.snapshot().await;
    let expected = table.rows().await;
    let mut writer = table.open(options()).await;
    writer.write(batch).await.unwrap();
    writer.abort().await.unwrap();
    assert_eq!(table.snapshot().await, before);
    assert_eq!(table.rows().await, expected);
}

#[tokio::test]
async fn empty_commit_keeps_table_empty() {
    let table = TestTable::new(TableSpec::default()).await;
    let before = table.snapshot().await;
    let writer = table.open(options()).await;
    writer.commit().await.unwrap();
    assert_eq!(table.snapshot().await, before);
    assert_eq!(table.rows().await.num_rows(), 0);
}

#[rstest::rstest]
#[case::small_target(Some(1), None, 3)]
#[case::table_target(None, Some("1"), 3)]
#[case::explicit_target_overrides_table(Some(usize::MAX), Some("1"), 1)]
#[tokio::test]
async fn target_file_size_rolls_files_without_losing_rows(
    #[case] target_file_size: Option<usize>,
    #[case] table_target: Option<&str>,
    #[case] file_count: usize,
) {
    let properties = table_target
        .map(|size| ("write.target-file-size-bytes".to_owned(), size.to_owned()))
        .into_iter()
        .collect();
    let table = TestTable::new(TableSpec {
        properties,
        ..Default::default()
    })
    .await;
    let batch = batch(table.schema.clone());
    let mut write_options = options();
    write_options.target_file_size = target_file_size;
    let mut writer = table.open(write_options).await;
    for _ in 0..3 {
        writer.write(batch.clone()).await.unwrap();
    }
    writer.commit().await.unwrap();
    assert_eq!(table.files().await.len(), file_count);
    table.assert_rows(&vec![batch; 3]).await;
}

#[tokio::test]
async fn identity_partitions_round_trip_and_record_metadata() {
    let table = TestTable::new(TableSpec {
        partition: Some(
            UnboundPartitionSpec::builder()
                .add_partition_field(1, "id_partition", Transform::Identity)
                .unwrap()
                .add_partition_field(2, "value_partition", Transform::Identity)
                .unwrap()
                .build(),
        ),
        ..Default::default()
    })
    .await;
    let batch = RecordBatch::try_new(
        table.schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![
                Some(1),
                Some(2),
                Some(1),
                None,
                Some(1),
                Some(1),
            ])),
            Arc::new(Int64Array::from(vec![
                Some(10),
                Some(20),
                Some(11),
                Some(30),
                None,
                Some(10),
            ])),
        ],
    )
    .unwrap();
    let expected: HashSet<Struct> = [
        (Some(1), Some(10)),
        (Some(2), Some(20)),
        (Some(1), Some(11)),
        (None, Some(30)),
        (Some(1), None),
    ]
    .into_iter()
    .map(|(id, value)| Struct::from_iter([id.map(Literal::int), value.map(Literal::long)]))
    .collect();
    partitioned_round_trip(
        &table,
        &batch,
        PartitionType::Identity,
        &[(0, 0), (0, 4), (4, 2)],
        expected,
    )
    .await;
}

async fn partition_writer(
    transform: Transform,
    partition_type: PartitionType,
) -> (
    TestTable,
    RecordBatch,
    Box<dyn open_arrow_sink_core::Writer>,
) {
    let spec = UnboundPartitionSpec::builder()
        .add_partition_field(1, "id_partition", transform)
        .unwrap()
        .build();
    let table = TestTable::new(TableSpec {
        partition: Some(spec),
        ..Default::default()
    })
    .await;
    let data = batch(table.schema.clone());
    let writer = table
        .open(
            options()
                .with_target_file_size(1)
                .with_partition(PartitionOptions { partition_type }),
        )
        .await;
    (table, data, writer)
}

#[rstest::rstest]
#[case::identity(Transform::Identity, PartitionType::Identity)]
#[case::void(Transform::Void, PartitionType::Void)]
#[tokio::test]
async fn partitioned_empty_sessions_do_not_publish(
    #[case] transform: Transform,
    #[case] partition_type: PartitionType,
) {
    let (table, data, mut writer) = partition_writer(transform, partition_type).await;
    writer.write(data.slice(0, 0)).await.unwrap();
    writer.commit().await.unwrap();
    assert!(table.snapshot().await.is_none());
}

#[rstest::rstest]
#[case::identity(Transform::Identity, PartitionType::Identity, 6)]
#[case::void(Transform::Void, PartitionType::Void, 2)]
#[tokio::test]
async fn partitioned_file_rolling_preserves_rows(
    #[case] transform: Transform,
    #[case] partition_type: PartitionType,
    #[case] file_count: usize,
) {
    let (table, data, mut writer) = partition_writer(transform, partition_type).await;
    writer.write(data.slice(0, 0)).await.unwrap();
    writer.write(data.clone()).await.unwrap();
    writer.write(data.clone()).await.unwrap();
    writer.commit().await.unwrap();
    assert_eq!(table.files().await.len(), file_count);
    table.assert_rows(&[data.clone(), data]).await;
}

// Before the epoch, two values in the same year, next year, and null.
fn timestamp_values() -> ArrayRef {
    Arc::new(TimestampMicrosecondArray::from(vec![
        Some(-1),
        Some(0),
        Some(3_600_000_000),
        Some(31_536_000_000_000),
        None,
    ]))
}

#[rstest::rstest]
#[case::timestamp_year(Transform::Year, PartitionType::Year, PrimitiveType::Timestamp, timestamp_values(), [-1, 0, 0, 1])]
#[case::timestamp_month(Transform::Month, PartitionType::Month, PrimitiveType::Timestamp, timestamp_values(), [-1, 0, 0, 12])]
#[case::date_day(Transform::Day, PartitionType::Day, PrimitiveType::Date, Arc::new(Date32Array::from(vec![Some(-1), Some(0), Some(1), Some(365), None])) as ArrayRef, [-1, 0, 1, 365])]
#[case::timestamp_hour(Transform::Hour, PartitionType::Hour, PrimitiveType::Timestamp, timestamp_values(), [-1, 0, 1, 8760])]
#[tokio::test]
async fn temporal_partitions_round_trip_and_record_metadata(
    #[case] transform: Transform,
    #[case] partition_type: PartitionType,
    #[case] value_type: PrimitiveType,
    #[case] values: ArrayRef,
    #[case] expected_keys: [i32; 4],
) {
    let spec = UnboundPartitionSpec::builder()
        .add_partition_field(2, "time_partition", transform)
        .unwrap()
        .build();
    let table = TestTable::new(TableSpec {
        partition: Some(spec),
        value_type,
        ..Default::default()
    })
    .await;
    let data = RecordBatch::try_new(
        table.schema.clone(),
        vec![Arc::new(Int32Array::from(vec![0, 1, 2, 3, 4])), values],
    )
    .unwrap();
    let expected: HashSet<_> = expected_keys
        .into_iter()
        .map(|key| Struct::from_iter([Some(Literal::int(key))]))
        .chain([Struct::from_iter([None])])
        .collect();
    // Revisit partitions across batch boundaries and preserve the repeated row.
    partitioned_round_trip(
        &table,
        &data,
        partition_type,
        &[(0, 0), (0, 3), (3, 2), (1, 1)],
        expected,
    )
    .await;
}

#[rstest::rstest]
#[case::bucket(Transform::Bucket(16), PartitionType::Bucket, vec![Some(34), Some(34), None], vec![Some(Literal::int(3)), None])]
#[case::truncate(Transform::Truncate(10), PartitionType::Truncate, vec![Some(-1), Some(21), Some(29), None], vec![Some(Literal::long(-10)), Some(Literal::long(20)), None])]
#[case::void(Transform::Void, PartitionType::Void, vec![Some(-1), Some(21), Some(29), None], vec![None])]
#[tokio::test]
async fn numeric_partitions_round_trip_and_record_metadata(
    #[case] transform: Transform,
    #[case] partition_type: PartitionType,
    #[case] values: Vec<Option<i64>>,
    #[case] keys: Vec<Option<Literal>>,
) {
    let spec = UnboundPartitionSpec::builder()
        .add_partition_field(2, "value_partition", transform)
        .unwrap()
        .build();
    let table = TestTable::new(TableSpec {
        partition: Some(spec),
        ..Default::default()
    })
    .await;
    let ids: Vec<i32> = (0..values.len() as i32).collect();
    let data = RecordBatch::try_new(
        table.schema.clone(),
        vec![
            Arc::new(Int32Array::from(ids.clone())),
            Arc::new(Int64Array::from(values.clone())),
        ],
    )
    .unwrap();
    let expected: HashSet<_> = keys
        .into_iter()
        .map(|key| Struct::from_iter([key]))
        .collect();
    partitioned_round_trip(
        &table,
        &data,
        partition_type,
        &[(0, 0), (0, 1), (1, data.num_rows() - 1)],
        expected,
    )
    .await;
}

#[rstest::rstest]
#[case(
    open_arrow_sink_core::CompressionSpec::Uncompressed,
    parquet::basic::Compression::UNCOMPRESSED
)]
#[case(
    open_arrow_sink_core::CompressionSpec::Snappy,
    parquet::basic::Compression::SNAPPY
)]
#[case(
    open_arrow_sink_core::CompressionSpec::Gzip,
    parquet::basic::Compression::GZIP(Default::default())
)]
#[case(
    open_arrow_sink_core::CompressionSpec::Zstd { level: None },
    parquet::basic::Compression::ZSTD(Default::default())
)]
#[case(
    open_arrow_sink_core::CompressionSpec::Lz4Raw,
    parquet::basic::Compression::LZ4_RAW
)]
#[tokio::test]
async fn compression_survives_rolling_and_round_trips(
    #[case] codec: open_arrow_sink_core::CompressionSpec,
    #[case] expected: parquet::basic::Compression,
) {
    use parquet::file::reader::{FileReader, SerializedFileReader};
    let table = TestTable::new(TableSpec::default()).await;
    let before = table.load().await;
    let mut options = options();
    options.compression.global = Some(codec);
    options.target_file_size = Some(1);
    let mut writer = table.open(options).await;
    writer.write(batch(table.schema.clone())).await.unwrap();
    writer.write(batch(table.schema.clone())).await.unwrap();
    writer.commit().await.unwrap();
    let loaded = table.load().await;
    assert_eq!(
        loaded.metadata().properties(),
        before.metadata().properties()
    );
    let files = files(&loaded).await;
    assert!(!files.is_empty());
    for file in files {
        let bytes = loaded
            .file_io()
            .new_input(file.data_file_path())
            .unwrap()
            .read()
            .await
            .unwrap();
        let reader = SerializedFileReader::new(bytes).unwrap();
        for group in reader.metadata().row_groups() {
            for column in group.columns() {
                assert_eq!(column.compression(), expected);
            }
        }
    }
    assert_eq!(table.rows().await.num_rows(), 6);
}

#[tokio::test]
async fn unordered_commit_preserves_all_rows() {
    use open_arrow_sink_core::SinkExt;
    use open_arrow_sink_layer::ParallelIngestionLayer;
    use std::num::NonZeroUsize;

    let table = TestTable::new(TableSpec::default()).await;
    let mut writer = table
        .sink()
        .layer(ParallelIngestionLayer::fixed(NonZeroUsize::new(3).unwrap()))
        .open(table.identifier.clone(), table.schema.clone(), options())
        .await
        .unwrap();
    let mut expected = Vec::new();
    for id in 0..8 {
        let batch = RecordBatch::try_new(
            table.schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![Some(id * 2), Some(id * 2 + 1), None])),
                Arc::new(Int64Array::from(vec![Some(i64::from(id)), None, Some(-1)])),
            ],
        )
        .unwrap();
        writer.write(batch.clone()).await.unwrap();
        expected.push(batch);
    }
    writer.commit().await.unwrap();
    table.assert_rows(&expected).await;
}
