use std::sync::Arc;

use iceberg::Catalog;
use iceberg::table::Table;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::writer::IcebergWriter;
use open_arrow_sink_core::{
    RecordBatch, Result, SchemaRef, WriteSummary, Writer, async_trait, validation,
};

use crate::{
    error::backend,
    validation::{validate_commit_state, validate_write_state},
};

pub(crate) struct IcebergTableWriter {
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
        validate_write_state(self.failed)?;
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
        validate_commit_state(self.failed)?;
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

impl IcebergTableWriter {
    pub(crate) fn new(
        catalog: Arc<dyn Catalog>,
        table: Table,
        schema: SchemaRef,
        prefix: String,
        inner: Box<dyn IcebergWriter>,
    ) -> Self {
        Self {
            catalog,
            table,
            schema,
            prefix,
            inner,
            rows: 0,
            failed: false,
        }
    }
}
