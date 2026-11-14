use std::{collections::HashMap, sync::Arc};

use arrow_array::{Array, Int32Array, Int64Array, RecordBatch};
use futures::TryStreamExt;
use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableCreation, TableIdent};
use open_arrow_sink_core::{Ordering, SchemaRef, Sink, WriteOptions};
use open_arrow_sink_iceberg::{IcebergTableSink, iceberg};

type Row = (Option<i32>, Option<i64>);

async fn table() -> (Arc<dyn Catalog>, TableIdent, SchemaRef) {
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
                Type::Primitive(PrimitiveType::Long),
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

#[tokio::test]
async fn target_file_size_rolls_files_without_losing_rows() {
    for (target_file_size, file_count) in [(None, 1), (Some(1), 3)] {
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
}
