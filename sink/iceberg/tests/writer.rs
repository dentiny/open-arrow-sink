use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use arrow_array::{
    Array, ArrayRef, Date32Array, Int32Array, Int64Array, RecordBatch, TimestampMicrosecondArray,
};
use futures::TryStreamExt;
use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
use iceberg::spec::{
    Literal, NestedField, PrimitiveType, Schema, Struct, Transform, Type, UnboundPartitionSpec,
};
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableCreation, TableIdent};
use open_arrow_sink_core::{
    Ordering, PartitionOptions, PartitionType, SchemaRef, Sink, WriteOptions,
};
use open_arrow_sink_iceberg::{IcebergTableSink, iceberg};

type Row = (Option<i32>, Option<i64>);

async fn table() -> (Arc<dyn Catalog>, TableIdent, SchemaRef) {
    table_with_spec(None).await
}

async fn table_with_spec(
    spec: Option<UnboundPartitionSpec>,
) -> (Arc<dyn Catalog>, TableIdent, SchemaRef) {
    table_with_value_type(spec, PrimitiveType::Long).await
}

async fn table_with_value_type(
    spec: Option<UnboundPartitionSpec>,
    value_type: PrimitiveType,
) -> (Arc<dyn Catalog>, TableIdent, SchemaRef) {
    let catalog: Arc<dyn Catalog> = Arc::new(
        MemoryCatalogBuilder::default()
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
                Type::Primitive(value_type),
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
                .partition_spec_opt(spec)
                .build(),
        )
        .await
        .unwrap();
    let schema = Arc::new(
        iceberg::arrow::schema_to_arrow_schema(table.metadata().current_schema()).unwrap(),
    );
    (
        catalog,
        TableIdent::new(namespace, "numbers".into()),
        schema,
    )
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
    WriteOptions {
        mode: open_arrow_sink_core::WriteMode::Append,
        ordering: Ordering::Unordered,
        ..WriteOptions::default()
    }
}

async fn rows(catalog: &dyn Catalog, identifier: &TableIdent) -> Vec<Row> {
    let table = catalog.load_table(identifier).await.unwrap();
    let batches: Vec<RecordBatch> = table
        .scan()
        .build()
        .unwrap()
        .to_arrow()
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let mut rows = Vec::new();
    for batch in batches {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let values = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for i in 0..batch.num_rows() {
            rows.push((
                (!ids.is_null(i)).then(|| ids.value(i)),
                (!values.is_null(i)).then(|| values.value(i)),
            ));
        }
    }
    rows.sort_unstable();
    rows
}

#[tokio::test]
async fn commits_round_trip_batches_and_preserve_existing_rows() {
    let (catalog, identifier, schema) = table().await;
    let sink = IcebergTableSink::new(catalog.clone());
    let batch = batch(schema.clone());
    let mut writer = sink
        .open(identifier.clone(), schema.clone(), options())
        .await
        .unwrap();
    assert_eq!(writer.schema(), &schema);
    writer.write(batch.slice(0, 1)).await.unwrap();
    writer.write(batch.slice(1, 2)).await.unwrap();
    assert_eq!(writer.commit().await.unwrap().rows_written, 3);
    let expected = vec![(None, Some(20)), (Some(1), None), (Some(3), Some(30))];
    assert_eq!(rows(catalog.as_ref(), &identifier).await, expected);

    let mut writer = sink
        .open(identifier.clone(), schema, options())
        .await
        .unwrap();
    writer.write(batch.slice(0, 1)).await.unwrap();
    assert_eq!(writer.commit().await.unwrap().rows_written, 1);
    let mut expected = expected;
    expected.push((Some(3), Some(30)));
    expected.sort_unstable();
    assert_eq!(rows(catalog.as_ref(), &identifier).await, expected);
}

#[tokio::test]
async fn abort_preserves_snapshot_and_committed_content() {
    let (catalog, identifier, schema) = table().await;
    let sink = IcebergTableSink::new(catalog.clone());
    let batch = batch(schema.clone());
    let mut writer = sink
        .open(identifier.clone(), schema.clone(), options())
        .await
        .unwrap();
    writer.write(batch.clone()).await.unwrap();
    writer.commit().await.unwrap();
    let before = catalog.load_table(&identifier).await.unwrap();
    let expected = rows(catalog.as_ref(), &identifier).await;
    let mut writer = sink
        .open(identifier.clone(), schema, options())
        .await
        .unwrap();
    writer.write(batch).await.unwrap();
    writer.abort().await.unwrap();
    let after = catalog.load_table(&identifier).await.unwrap();
    assert_eq!(
        after.metadata().current_snapshot_id(),
        before.metadata().current_snapshot_id()
    );
    assert_eq!(rows(catalog.as_ref(), &identifier).await, expected);
}

#[tokio::test]
async fn empty_commit_keeps_table_empty() {
    let (catalog, identifier, schema) = table().await;
    let before = catalog.load_table(&identifier).await.unwrap();
    let writer = IcebergTableSink::new(catalog.clone())
        .open(identifier.clone(), schema, options())
        .await
        .unwrap();
    assert_eq!(writer.commit().await.unwrap().rows_written, 0);
    let after = catalog.load_table(&identifier).await.unwrap();
    assert_eq!(
        after.metadata().current_snapshot_id(),
        before.metadata().current_snapshot_id()
    );
    assert!(rows(catalog.as_ref(), &identifier).await.is_empty());
}

#[rstest::rstest]
#[case::default_size(None, 1)]
#[case::small_target(Some(1), 3)]
#[tokio::test]
async fn target_file_size_rolls_files_without_losing_rows(
    #[case] target_file_size: Option<usize>,
    #[case] file_count: usize,
) {
    let (catalog, identifier, schema) = table().await;
    let batch = batch(schema.clone());
    let mut writer = IcebergTableSink::new(catalog.clone())
        .open(
            identifier.clone(),
            schema,
            WriteOptions {
                target_file_size,
                ..options()
            },
        )
        .await
        .unwrap();
    for _ in 0..3 {
        writer.write(batch.clone()).await.unwrap();
    }
    assert_eq!(writer.commit().await.unwrap().rows_written, 9);
    let table = catalog.load_table(&identifier).await.unwrap();
    let files: Vec<_> = table
        .scan()
        .build()
        .unwrap()
        .plan_files()
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    assert_eq!(files.len(), file_count);
    let mut expected = [(None, Some(20)), (Some(1), None), (Some(3), Some(30))].repeat(3);
    expected.sort_unstable();
    assert_eq!(rows(catalog.as_ref(), &identifier).await, expected);
}

fn identity_spec(composite: bool) -> UnboundPartitionSpec {
    let builder = UnboundPartitionSpec::builder()
        .add_partition_field(1, "id_partition", Transform::Identity)
        .unwrap();
    if composite {
        builder
            .add_partition_field(2, "value_partition", Transform::Identity)
            .unwrap()
            .build()
    } else {
        builder.build()
    }
}

fn partition_options() -> WriteOptions {
    WriteOptions {
        partition: Some(PartitionOptions::default()),
        ..options()
    }
}

#[rstest::rstest]
#[case::single(false)]
#[case::composite(true)]
#[tokio::test]
async fn identity_partitions_round_trip_and_record_metadata(#[case] composite: bool) {
    let (catalog, identifier, schema) = table_with_spec(Some(identity_spec(composite))).await;
    let batch = RecordBatch::try_new(
        schema.clone(),
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
    let mut writer = IcebergTableSink::new(catalog.clone())
        .open(identifier.clone(), schema, partition_options())
        .await
        .unwrap();
    writer.write(batch.slice(0, 0)).await.unwrap();
    writer.write(batch.slice(0, 4)).await.unwrap();
    writer.write(batch.slice(4, 2)).await.unwrap();
    assert!(
        catalog
            .load_table(&identifier)
            .await
            .unwrap()
            .metadata()
            .current_snapshot_id()
            .is_none()
    );
    assert_eq!(writer.commit().await.unwrap().rows_written, 6);
    assert_eq!(
        rows(catalog.as_ref(), &identifier).await,
        vec![
            (None, Some(30)),
            (Some(1), None),
            (Some(1), Some(10)),
            (Some(1), Some(10)),
            (Some(1), Some(11)),
            (Some(2), Some(20)),
        ]
    );
    let table = catalog.load_table(&identifier).await.unwrap();
    let files: Vec<_> = table
        .scan()
        .build()
        .unwrap()
        .plan_files()
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let expected: HashSet<Struct> = if composite {
        [
            (Some(1), Some(10)),
            (Some(2), Some(20)),
            (Some(1), Some(11)),
            (None, Some(30)),
            (Some(1), None),
        ]
        .into_iter()
        .map(|(id, value)| Struct::from_iter([id.map(Literal::int), value.map(Literal::long)]))
        .collect()
    } else {
        [Some(1), Some(2), None]
            .into_iter()
            .map(|id| Struct::from_iter([id.map(Literal::int)]))
            .collect()
    };
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

#[rstest::rstest]
#[case::identity_empty(Transform::Identity, PartitionType::Identity, true, 0)]
#[case::identity_rolling(Transform::Identity, PartitionType::Identity, false, 6)]
#[case::void_empty(Transform::Void, PartitionType::Void, true, 0)]
#[case::void_rolling(Transform::Void, PartitionType::Void, false, 2)]
#[tokio::test]
async fn partitioned_empty_sessions_and_file_rolling(
    #[case] transform: Transform,
    #[case] partition_type: PartitionType,
    #[case] empty: bool,
    #[case] file_count: usize,
) {
    let spec = UnboundPartitionSpec::builder()
        .add_partition_field(1, "id_partition", transform)
        .unwrap()
        .build();
    let (catalog, identifier, schema) = table_with_spec(Some(spec)).await;
    let data = batch(schema.clone());
    let mut writer = IcebergTableSink::new(catalog.clone())
        .open(
            identifier.clone(),
            schema,
            WriteOptions {
                target_file_size: Some(1),
                partition: Some(PartitionOptions { partition_type }),
                ..options()
            },
        )
        .await
        .unwrap();
    writer.write(data.slice(0, 0)).await.unwrap();
    if !empty {
        writer.write(data.clone()).await.unwrap();
        writer.write(data).await.unwrap();
    }
    assert_eq!(
        writer.commit().await.unwrap().rows_written,
        if empty { 0 } else { 6 }
    );
    let table = catalog.load_table(&identifier).await.unwrap();
    if empty {
        assert!(table.metadata().current_snapshot_id().is_none());
    } else {
        let files: Vec<_> = table
            .scan()
            .build()
            .unwrap()
            .plan_files()
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        assert_eq!(files.len(), file_count);
        let mut expected = [(None, Some(20)), (Some(1), None), (Some(3), Some(30))].repeat(2);
        expected.sort_unstable();
        assert_eq!(rows(catalog.as_ref(), &identifier).await, expected);
    }
}

#[rstest::rstest]
#[case::timestamp_year(Transform::Year, PartitionType::Year, false, [-1, 0, 0, 1])]
#[case::date_year(Transform::Year, PartitionType::Year, true, [-1, 0, 0, 1])]
#[case::timestamp_month(Transform::Month, PartitionType::Month, false, [-1, 0, 0, 12])]
#[case::date_month(Transform::Month, PartitionType::Month, true, [-1, 0, 0, 12])]
#[case::timestamp_day(Transform::Day, PartitionType::Day, false, [-1, 0, 0, 365])]
#[case::date_day(Transform::Day, PartitionType::Day, true, [-1, 0, 1, 365])]
#[case::timestamp_hour(Transform::Hour, PartitionType::Hour, false, [-1, 0, 1, 8760])]
#[tokio::test]
async fn temporal_partitions_round_trip_and_record_metadata(
    #[case] transform: Transform,
    #[case] partition_type: PartitionType,
    #[case] date: bool,
    #[case] expected_keys: [i32; 4],
) {
    let spec = UnboundPartitionSpec::builder()
        .add_partition_field(2, "time_partition", transform)
        .unwrap()
        .build();
    let value_type = if date {
        PrimitiveType::Date
    } else {
        PrimitiveType::Timestamp
    };
    let (catalog, identifier, schema) = table_with_value_type(Some(spec), value_type).await;
    // Before the epoch, two values in the same year, next year, and null.
    let times = if date {
        vec![Some(-1), Some(0), Some(1), Some(365), None]
    } else {
        vec![
            Some(-1),
            Some(0),
            Some(3_600_000_000),
            Some(31_536_000_000_000),
            None,
        ]
    };
    let values: ArrayRef = if date {
        Arc::new(Date32Array::from(
            times
                .iter()
                .map(|v| v.map(|v| v as i32))
                .collect::<Vec<_>>(),
        ))
    } else {
        Arc::new(TimestampMicrosecondArray::from(times.clone()))
    };
    let data = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int32Array::from(vec![0, 1, 2, 3, 4])), values],
    )
    .unwrap();
    let mut writer = IcebergTableSink::new(catalog.clone())
        .open(
            identifier.clone(),
            schema,
            WriteOptions {
                partition: Some(PartitionOptions { partition_type }),
                ..options()
            },
        )
        .await
        .unwrap();
    writer.write(data.slice(0, 0)).await.unwrap();
    // Revisit partitions across batch boundaries.
    writer.write(data.slice(0, 3)).await.unwrap();
    writer.write(data.slice(3, 2)).await.unwrap();
    writer.write(data.slice(1, 1)).await.unwrap();
    assert_eq!(writer.commit().await.unwrap().rows_written, 6);
    let table = catalog.load_table(&identifier).await.unwrap();
    let files: Vec<_> = table
        .scan()
        .build()
        .unwrap()
        .plan_files()
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let expected: HashSet<_> = expected_keys
        .into_iter()
        .map(|key| Struct::from_iter([Some(Literal::int(key))]))
        .chain([Struct::from_iter([None])])
        .collect();
    assert_eq!(files.len(), expected.len());
    assert_eq!(
        files
            .iter()
            .map(|task| task.partition().unwrap().clone())
            .collect::<HashSet<_>>(),
        expected
    );
    let batches: Vec<RecordBatch> = table
        .scan()
        .build()
        .unwrap()
        .to_arrow()
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let mut actual = Vec::new();
    for batch in batches {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let values: Vec<Option<i64>> = if date {
            batch
                .column(1)
                .as_any()
                .downcast_ref::<Date32Array>()
                .unwrap()
                .iter()
                .map(|v| v.map(i64::from))
                .collect()
        } else {
            batch
                .column(1)
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .unwrap()
                .iter()
                .collect()
        };
        actual.extend((0..batch.num_rows()).map(|i| (ids.value(i), values[i])));
    }
    actual.sort_unstable();
    let mut expected: Vec<_> = times
        .into_iter()
        .enumerate()
        .map(|(i, value)| (i as i32, value))
        .collect();
    expected.push((1, Some(0)));
    expected.sort_unstable();
    assert_eq!(actual, expected);
}

#[rstest::rstest]
#[case::sixteen_buckets(16, 3)]
#[case::ten_buckets(10, 9)]
#[tokio::test]
async fn bucket_partitions_round_trip_and_record_metadata(
    #[case] buckets: u32,
    #[case] expected_bucket: i32,
) {
    assert_numeric_partition_round_trip(
        Transform::Bucket(buckets),
        PartitionType::Bucket,
        vec![Some(34), Some(34), None],
        vec![Some(Literal::int(expected_bucket)), None],
    )
    .await;
}

#[rstest::rstest]
#[case::width_ten(10, -10, 20)]
#[case::width_hundred(100, -100, 0)]
#[tokio::test]
async fn truncate_partitions_round_trip_and_record_metadata(
    #[case] width: u32,
    #[case] negative_key: i64,
    #[case] positive_key: i64,
) {
    assert_numeric_partition_round_trip(
        Transform::Truncate(width),
        PartitionType::Truncate,
        vec![Some(-1), Some(21), Some(29), None],
        vec![
            Some(Literal::long(negative_key)),
            Some(Literal::long(positive_key)),
            None,
        ],
    )
    .await;
}

#[tokio::test]
async fn void_partitions_round_trip_and_record_metadata() {
    assert_numeric_partition_round_trip(
        Transform::Void,
        PartitionType::Void,
        vec![Some(-1), Some(21), Some(29), None],
        vec![None],
    )
    .await;
}

async fn assert_numeric_partition_round_trip(
    transform: Transform,
    partition_type: PartitionType,
    values: Vec<Option<i64>>,
    keys: Vec<Option<Literal>>,
) {
    let spec = UnboundPartitionSpec::builder()
        .add_partition_field(2, "value_partition", transform)
        .unwrap()
        .build();
    let (catalog, identifier, schema) = table_with_spec(Some(spec)).await;
    let ids: Vec<i32> = (0..values.len() as i32).collect();
    let data = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(ids.clone())),
            Arc::new(Int64Array::from(values.clone())),
        ],
    )
    .unwrap();
    let mut writer = IcebergTableSink::new(catalog.clone())
        .open(
            identifier.clone(),
            schema,
            WriteOptions {
                partition: Some(PartitionOptions { partition_type }),
                ..options()
            },
        )
        .await
        .unwrap();
    writer.write(data.slice(0, 0)).await.unwrap();
    writer.write(data.slice(0, 1)).await.unwrap();
    writer
        .write(data.slice(1, data.num_rows() - 1))
        .await
        .unwrap();
    assert_eq!(
        writer.commit().await.unwrap().rows_written,
        values.len() as u64
    );
    let expected_rows = ids.into_iter().map(Some).zip(values).collect::<Vec<_>>();
    assert_eq!(rows(catalog.as_ref(), &identifier).await, expected_rows);
    let table = catalog.load_table(&identifier).await.unwrap();
    let files: Vec<_> = table
        .scan()
        .build()
        .unwrap()
        .plan_files()
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let expected: HashSet<_> = keys
        .into_iter()
        .map(|key| Struct::from_iter([key]))
        .collect();
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

#[rstest::rstest]
#[case(
    open_arrow_sink_core::Compression::Uncompressed,
    parquet::basic::Compression::UNCOMPRESSED
)]
#[case(
    open_arrow_sink_core::Compression::Snappy,
    parquet::basic::Compression::SNAPPY
)]
#[case(
    open_arrow_sink_core::Compression::Gzip,
    parquet::basic::Compression::GZIP(Default::default())
)]
#[case(
    open_arrow_sink_core::Compression::Zstd { level: None },
    parquet::basic::Compression::ZSTD(Default::default())
)]
#[case(
    open_arrow_sink_core::Compression::Lz4Raw,
    parquet::basic::Compression::LZ4_RAW
)]
#[tokio::test]
async fn compression_survives_rolling_and_round_trips(
    #[case] codec: open_arrow_sink_core::Compression,
    #[case] expected: parquet::basic::Compression,
) {
    use parquet::file::reader::{FileReader, SerializedFileReader};
    let (catalog, identifier, schema) = table().await;
    let before = catalog.load_table(&identifier).await.unwrap();
    let sink = IcebergTableSink::new(catalog.clone());
    let mut options = options();
    options.compression.global = Some(codec);
    options.target_file_size = Some(1);
    let mut writer = sink
        .open(identifier.clone(), schema.clone(), options)
        .await
        .unwrap();
    writer.write(batch(schema.clone())).await.unwrap();
    writer.write(batch(schema)).await.unwrap();
    writer.commit().await.unwrap();
    let table = catalog.load_table(&identifier).await.unwrap();
    assert_eq!(
        table.metadata().properties(),
        before.metadata().properties()
    );
    let files: Vec<_> = table
        .scan()
        .build()
        .unwrap()
        .plan_files()
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    assert!(!files.is_empty());
    for file in files {
        let bytes = table
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
    assert_eq!(rows(catalog.as_ref(), &identifier).await.len(), 6);
}
