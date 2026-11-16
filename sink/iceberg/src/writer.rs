use iceberg::table::Table;
use iceberg::writer::IcebergWriter;
use open_arrow_sink_core::{
    DataWriter, Error, Poison, RecordBatch, Result, SchemaRef, WriteOptions, async_trait,
    validation,
};

pub(crate) struct IcebergTableWriter {
    table: Table,
    schema: SchemaRef,
    prefix: String,
    inner: Box<dyn IcebergWriter>,
    rows: u64,
    state: Poison,
    options: WriteOptions,
}

#[async_trait]
impl DataWriter for IcebergTableWriter {
    type Output = (u64, Vec<iceberg::spec::DataFile>);
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    async fn write(&mut self, batch: RecordBatch) -> Result<()> {
        self.state.begin()?;
        validation::validate_batch(&self.schema, &batch)?;
        let rows = batch.num_rows() as u64;
        self.inner.write(batch).await.map_err(Error::backend)?;
        self.rows += rows;
        self.state.complete();
        Ok(())
    }

    async fn finish(&mut self) -> Result<Self::Output> {
        self.state.begin()?;
        let files = self.inner.close().await.map_err(Error::backend)?;
        Ok((self.rows, files))
    }
    async fn abort(self: Box<Self>) -> Result<()> {
        let Self {
            table,
            inner,
            prefix,
            options,
            ..
        } = *self;
        drop(inner);
        if options.cleanup_on_error {
            table
                .file_io()
                .delete_prefix(prefix)
                .await
                .map_err(Error::backend)
        } else {
            Ok(())
        }
    }
}

impl IcebergTableWriter {
    pub(crate) fn new(
        table: Table,
        schema: SchemaRef,
        prefix: String,
        inner: Box<dyn IcebergWriter>,
        options: WriteOptions,
    ) -> Self {
        Self {
            table,
            schema,
            prefix,
            inner,
            rows: 0,
            state: Poison::default(),
            options,
        }
    }
}
