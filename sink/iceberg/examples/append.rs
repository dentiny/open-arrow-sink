//! Run with: cargo run -p open-arrow-sink-iceberg --example append
use std::{collections::HashMap, sync::Arc};

use arrow_array::{Int32Array, RecordBatch};
use futures::TryStreamExt;
use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableCreation, TableIdent};
use open_arrow_sink_core::{Ordering, Sink, WriteMode, WriteOptions};
use open_arrow_sink_iceberg::{IcebergTableSink, iceberg};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Applications configure their catalog and create the table separately.
    let catalog = Arc::new(
        MemoryCatalogBuilder::default()
            .load(
                "example",
                HashMap::from([(
                    MEMORY_CATALOG_WAREHOUSE.into(),
                    "memory:///warehouse".into(),
                )]),
            )
            .await?,
    );
    let namespace = NamespaceIdent::new("example".into());
    catalog.create_namespace(&namespace, HashMap::new()).await?;
    let schema = Schema::builder()
        .with_fields(vec![Arc::new(NestedField::required(
            1,
            "value",
            Type::Primitive(PrimitiveType::Int),
        ))])
        .build()?;
    let table = catalog
        .create_table(
            &namespace,
            TableCreation::builder()
                .name("numbers".into())
                .schema(schema)
                .build(),
        )
        .await?;
    let schema = Arc::new(iceberg::arrow::schema_to_arrow_schema(
        table.metadata().current_schema(),
    )?);
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
    )?;
    let sink = IcebergTableSink::new(catalog.clone());
    let identifier = TableIdent::new(namespace, "numbers".into());
    let mut writer = sink
        .open(
            identifier.clone(),
            schema,
            WriteOptions::default()
                .with_mode(WriteMode::Append)
                .with_ordering(Ordering::Unordered),
        )
        .await?;
    writer.write(batch).await?;
    writer.commit().await?;
    let table = catalog.load_table(&identifier).await?;
    let batches: Vec<RecordBatch> = table
        .scan()
        .build()?
        .to_arrow()
        .await?
        .try_collect()
        .await?;
    println!(
        "Read {} committed rows",
        batches.iter().map(RecordBatch::num_rows).sum::<usize>()
    );
    Ok(())
}
