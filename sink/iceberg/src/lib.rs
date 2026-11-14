//! Append batches to an existing, unpartitioned Iceberg table.
pub use iceberg;

use std::sync::Arc;

use iceberg::arrow::schema_to_arrow_schema;
use iceberg::spec::{PartitionKey, Struct};
use iceberg::table::Table;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator, LocationGenerator,
};
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use iceberg::{Catalog, TableIdent};
use open_arrow_sink_core::{
    Error, Ordering, RecordBatch, Result, SchemaRef, Sink, SinkCapability, WriteOptions,
    WriteSummary, Writer, async_trait, validation,
};

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
        if table.readonly() || !metadata.default_partition_spec().fields().is_empty() {
            return Err(Error::Unsupported(
                "only writable, unpartitioned tables are supported".into(),
            ));
        }
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
        Ok(Box::new(IcebergTableWriter {
            catalog: self.catalog.clone(),
            table,
            schema,
            prefix,
            inner: Box::new(inner),
            rows: 0,
            failed: false,
        }))
    }
}

struct IcebergTableWriter {
    catalog: Arc<dyn Catalog>,
    table: Table,
    schema: SchemaRef,
    prefix: String,
    inner: Box<dyn IcebergWriter>,
    rows: u64,
    failed: bool,
}

#[async_trait]
impl Writer for IcebergTableWriter {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    async fn write(&mut self, batch: RecordBatch) -> Result<()> {
        if self.failed {
            return Err(Error::Unsupported("abort or drop the failed writer".into()));
        }
        // Mark before awaiting so cancellation also makes this session unusable.
        self.failed = true;
        validation::validate_batch(&self.schema, &batch)?;
        let rows = batch.num_rows() as u64;
        self.inner.write(batch).await.map_err(backend)?;
        self.rows += rows;
        self.failed = false;
        Ok(())
    }

    async fn commit(mut self: Box<Self>) -> Result<WriteSummary> {
        if self.failed {
            return Err(Error::Unsupported("cannot commit a failed writer".into()));
        }
        let files = self.inner.close().await.map_err(backend)?;
        if !files.is_empty() {
            let tx = Transaction::new(&self.table);
            let tx = tx
                .fast_append()
                .add_data_files(files)
                .apply(tx)
                .map_err(backend)?;
            tx.commit(self.catalog.as_ref()).await.map_err(backend)?;
        }
        // On commit failure, retain files: the catalog may already have published them.
        Ok(WriteSummary {
            rows_written: self.rows,
        })
    }

    async fn abort(self: Box<Self>) -> Result<()> {
        let Self {
            table,
            inner,
            prefix,
            ..
        } = *self;
        drop(inner);
        table.file_io().delete_prefix(prefix).await.map_err(backend)
    }
}

fn backend(error: iceberg::Error) -> Error {
    Error::Backend(Box::new(error))
}
