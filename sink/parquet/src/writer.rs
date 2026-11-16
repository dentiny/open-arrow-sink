use open_arrow_sink_core::{
    DataWriter, Error, Poison, RecordBatch, Result, SchemaRef, async_trait, validation,
};
use parquet::{arrow::AsyncArrowWriter, file::properties::WriterProperties};
use std::sync::Arc;

use crate::{destination::Destination, file_writer::FileWriter};

pub(crate) struct ParquetDirectoryWriter {
    destination: Arc<Destination>,
    schema: SchemaRef,
    // Open lazily on the first nonempty batch. Poison gates every operation.
    writer: Option<AsyncArrowWriter<FileWriter>>,
    target_file_size: Option<usize>,
    state: Poison,
    properties: WriterProperties,
    rows_written: u64,
    files_created: usize,
    writer_index: usize,
}

#[async_trait]
impl DataWriter for ParquetDirectoryWriter {
    type Output = u64;
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    async fn write(&mut self, batch: RecordBatch) -> Result<()> {
        // Retain upload ownership for abort, including after cancellation.
        self.state.begin()?;
        validation::validate_batch(&self.schema, &batch)?;
        if batch.num_rows() == 0 {
            self.state.complete();
            return Ok(());
        }
        if self
            .writer
            .as_ref()
            .is_some_and(|writer| self.should_roll_file(writer))
        {
            self.writer
                .as_mut()
                .unwrap()
                .finish()
                .await
                .map_err(Error::backend)?;
            self.writer = None;
        }
        if self.writer.is_none() {
            let writer = self
                .destination
                .open_writer(
                    self.writer_index,
                    self.files_created,
                    self.schema.clone(),
                    self.properties.clone(),
                )
                .await?;
            self.files_created += 1;
            self.writer = Some(writer);
        }
        let writer = self
            .writer
            .as_mut()
            .expect("active writer owns a Parquet file");
        writer.write(&batch).await.map_err(Error::backend)?;
        // Let Parquet coalesce small batches into its bounded row groups.
        // Only force an early group boundary when buffered encoded data could
        // reach the file target; rolling still uses actual flushed bytes.
        if writer.in_progress_rows() > 0
            && self.target_file_size.is_some_and(|size| {
                writer
                    .bytes_written()
                    .saturating_add(writer.in_progress_size())
                    >= size
            })
        {
            writer.flush().await.map_err(Error::backend)?;
        }
        self.rows_written += batch.num_rows() as u64;
        self.state.complete();
        Ok(())
    }

    async fn finish(&mut self) -> Result<u64> {
        self.state.begin()?;
        if let Some(writer) = self.writer.as_mut() {
            writer.finish().await.map_err(Error::backend)?;
            self.writer = None;
        }
        Ok(self.rows_written)
    }
    async fn abort(mut self: Box<Self>) -> Result<()> {
        self.state.poison();
        match self.writer.take() {
            Some(writer) => writer.into_inner().0.abort().await,
            None => Ok(()),
        }
    }
}

impl ParquetDirectoryWriter {
    pub(crate) fn new(
        destination: Arc<Destination>,
        schema: SchemaRef,
        target_file_size: Option<usize>,
        properties: WriterProperties,
        writer_index: usize,
    ) -> Self {
        Self {
            destination,
            schema,
            writer: None,
            state: Poison::default(),
            target_file_size,
            properties,
            rows_written: 0,
            files_created: 0,
            writer_index,
        }
    }

    fn should_roll_file(&self, writer: &AsyncArrowWriter<FileWriter>) -> bool {
        !writer.flushed_row_groups().is_empty()
            && self
                .target_file_size
                .is_some_and(|size| writer.bytes_written() >= size)
    }
}
