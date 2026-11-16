use iceberg::writer::IcebergWriter;
use open_arrow_sink_core::{
    DataWriter, Error, Poison, RecordBatch, Result, SchemaRef, async_trait, validation,
};

pub(crate) struct IcebergTableWriter {
    schema: SchemaRef,
    inner: Box<dyn IcebergWriter>,
    rows: u64,
    state: Poison,
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
        Ok(())
    }
}

impl IcebergTableWriter {
    pub(crate) fn new(schema: SchemaRef, inner: Box<dyn IcebergWriter>) -> Self {
        Self {
            schema,
            inner,
            rows: 0,
            state: Poison::default(),
        }
    }
}
