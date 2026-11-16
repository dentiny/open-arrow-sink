use lance::dataset::WriteParams;
use open_arrow_sink_core::{
    DataWriter, Poison, RecordBatch, Result, SchemaRef, async_trait, validation,
};

use crate::{
    LanceFragments, compression::writer_batch, destination::LanceDestination,
    worker::FragmentWorker,
};

/// Writes uncommitted fragments. No worker can publish a dataset manifest.
pub(crate) struct FragmentWriter {
    schemas: crate::write_options::LanceSchemas,
    worker: FragmentWorker,
    rows: u64,
    state: Poison,
}

impl FragmentWriter {
    pub(crate) fn new(
        schemas: crate::write_options::LanceSchemas,
        destination: LanceDestination,
        params: WriteParams,
    ) -> Self {
        let worker = FragmentWorker::new(schemas.encoded.clone(), &destination, params);
        Self {
            schemas,
            worker,
            rows: 0,
            state: Poison::default(),
        }
    }
}

#[async_trait]
impl DataWriter for FragmentWriter {
    type Output = LanceFragments;
    fn schema(&self) -> &SchemaRef {
        &self.schemas.input
    }
    async fn write(&mut self, batch: RecordBatch) -> Result<()> {
        self.state.begin()?;
        validation::validate_batch(&self.schemas.input, &batch)?;
        let rows = batch.num_rows() as u64;
        let batch = writer_batch(batch, &self.schemas.encoded)?;
        self.worker.write(batch).await?;
        self.rows += rows;
        self.state.complete();
        Ok(())
    }
    async fn finish(&mut self) -> Result<Self::Output> {
        self.state.begin()?;
        let transaction = self.worker.finish().await?;
        Ok(LanceFragments {
            rows_written: self.rows,
            transaction,
        })
    }
    async fn abort(mut self: Box<Self>) -> Result<()> {
        self.worker.abort().await;
        // Session cleanup runs only after every worker has stopped.
        Ok(())
    }
}
