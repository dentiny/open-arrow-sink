use std::sync::Arc;

use iceberg::Catalog;
use iceberg::table::Table;
use iceberg::writer::IcebergWriter;
use open_arrow_sink_core::{
    DataWriter, RecordBatch, Result, SchemaRef, SessionCommitter, WriteSummary, Writer,
    async_trait, validation,
};

use crate::{
    committer::IcebergCommitter,
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
    cleanup_on_error: bool,
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
        Box::new(IcebergCommitter {
            catalog: self.catalog,
            table: self.table,
        })
        .commit(vec![(self.rows, files)])
        .await
    }

    async fn abort(self: Box<Self>) -> Result<()> {
        let Self {
            table,
            inner,
            prefix,
            cleanup_on_error,
            ..
        } = *self;
        drop(inner);
        if cleanup_on_error {
            table.file_io().delete_prefix(prefix).await.map_err(backend)
        } else {
            Ok(())
        }
    }
}

impl IcebergTableWriter {
    pub(crate) fn new(
        catalog: Arc<dyn Catalog>,
        table: Table,
        schema: SchemaRef,
        prefix: String,
        inner: Box<dyn IcebergWriter>,
        cleanup_on_error: bool,
    ) -> Self {
        Self {
            catalog,
            table,
            schema,
            prefix,
            inner,
            rows: 0,
            failed: false,
            cleanup_on_error,
        }
    }
}

#[async_trait]
impl DataWriter for IcebergTableWriter {
    type Output = (u64, Vec<iceberg::spec::DataFile>);
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }
    async fn write(&mut self, batch: RecordBatch) -> Result<()> {
        Writer::write(self, batch).await
    }
    async fn finish(&mut self) -> Result<Self::Output> {
        validate_commit_state(self.failed)?;
        self.failed = true;
        let files = self.inner.close().await.map_err(backend)?;
        Ok((self.rows, files))
    }
    async fn abort(self: Box<Self>) -> Result<()> {
        Writer::abort(self).await
    }
}
