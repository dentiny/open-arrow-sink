use std::sync::Arc;

use iceberg::arrow::schema_to_arrow_schema;
use iceberg::spec::{PartitionKey, Struct};
use iceberg::writer::IcebergWriterBuilder;
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator, LocationGenerator,
};
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::{Catalog, TableIdent};
use open_arrow_sink_core::{
    Error, Ordering, Result, SchemaRef, Sink, SinkCapability, WriteOptions, Writer, async_trait,
    validation,
};

use crate::{error::backend, validation::validate_table, writer::IcebergTableWriter};

/// Uses a caller-configured catalog; the table must already exist.
pub struct IcebergTableSink {
    catalog: Arc<dyn Catalog>,
}

impl IcebergTableSink {
    pub fn new(catalog: Arc<dyn Catalog>) -> Self {
        Self { catalog }
    }
}

#[async_trait]
impl Sink for IcebergTableSink {
    type Request = TableIdent;

    fn capability(&self) -> SinkCapability {
        SinkCapability {
            ordering: Ordering::Unordered,
        }
    }

    async fn open(
        &self,
        request: TableIdent,
        schema: SchemaRef,
        options: WriteOptions,
    ) -> Result<Box<dyn Writer>> {
        validation::validate_options(&options, self.capability())?;
        let table = self.catalog.load_table(&request).await.map_err(backend)?;
        let metadata = table.metadata();
        validate_table(&table)?;
        let table_schema = schema_to_arrow_schema(metadata.current_schema()).map_err(backend)?;
        if schema.as_ref() != &table_schema {
            return Err(Error::SchemaMismatch);
        }
        // A private session prefix lets abort remove only this writer's files.
        let id = uuid::Uuid::new_v4().to_string();
        let prefix = format!(
            "{}/",
            DefaultLocationGenerator::new(metadata)
                .map_err(backend)?
                .generate_location(None, &format!("open-arrow-sink-{id}"))
        );
        let parquet = ParquetWriterBuilder::from_table_properties(
            &metadata.table_properties(),
            metadata.current_schema().clone(),
        )
        .map_err(backend)?;
        let rolling = RollingFileWriterBuilder::new_with_default_file_size(
            parquet,
            table.file_io().clone(),
            DefaultLocationGenerator::with_data_location(prefix.trim_end_matches('/').to_owned()),
            DefaultFileNameGenerator::new(id, None, iceberg::spec::DataFileFormat::Parquet),
        );
        let partition = PartitionKey::new(
            metadata.default_partition_spec().as_ref().clone(),
            metadata.current_schema().clone(),
            Struct::empty(),
        );
        let inner = DataFileWriterBuilder::new(rolling)
            .build(Some(partition))
            .await
            .map_err(backend)?;
        Ok(Box::new(IcebergTableWriter::new(
            self.catalog.clone(),
            table,
            schema,
            prefix,
            Box::new(inner),
        )))
    }
}
