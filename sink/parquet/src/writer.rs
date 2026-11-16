use open_arrow_sink_core::{
    DataWriter, Error, Poison, RecordBatch, Result, SchemaRef, WriteOptions, async_trait,
    validation,
};
use parquet::{arrow::AsyncArrowWriter, file::properties::WriterProperties};
use std::sync::Arc;

use crate::{destination::Destination, error::closed, file_writer::FileWriter};

pub(crate) struct ParquetDirectoryWriter {
    pub(crate) destination: Arc<Destination>,
    schema: SchemaRef,
    writer: Option<AsyncArrowWriter<FileWriter>>,
    options: WriteOptions,
    state: Poison,
    properties: WriterProperties,
    rows_written: u64,
    files_created: usize,
    first_file: usize,
    file_stride: usize,
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
        if self.should_roll_file(self.writer.as_ref().ok_or_else(closed)?, batch.num_rows()) {
            self.writer
                .as_mut()
                .unwrap()
                .finish()
                .await
                .map_err(Error::backend)?;
            self.writer = None;
            let writer = self
                .destination
                .open_writer(
                    self.file_index(self.files_created),
                    self.schema.clone(),
                    self.properties.clone(),
                )
                .await?;
            self.files_created += 1;
            self.writer = Some(writer);
        }
        let writer = self.writer.as_mut().ok_or_else(closed)?;
        writer.write(&batch).await.map_err(Error::backend)?;
        // Let Parquet coalesce small batches into its bounded row groups.
        // Only force an early group boundary when buffered encoded data could
        // reach the file target; rolling still uses actual flushed bytes.
        if writer.in_progress_rows() > 0
            && self.options.target_file_size.is_some_and(|size| {
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
        self.writer
            .as_mut()
            .ok_or_else(closed)?
            .finish()
            .await
            .map_err(Error::backend)?;
        self.writer = None;
        self.destination.complete_reservation().await?;
        Ok(self.rows_written)
    }
    async fn abort(mut self: Box<Self>) -> Result<()> {
        self.state.poison();
        self.cleanup().await
    }
}

impl ParquetDirectoryWriter {
    pub(crate) fn new(
        destination: Arc<Destination>,
        schema: SchemaRef,
        writer: AsyncArrowWriter<FileWriter>,
        options: WriteOptions,
        properties: WriterProperties,
        first_file: usize,
        file_stride: usize,
    ) -> Self {
        Self {
            destination,
            schema,
            writer: Some(writer),
            state: Poison::default(),
            options,
            properties,
            rows_written: 0,
            files_created: 1,
            first_file,
            file_stride,
        }
    }

    fn should_roll_file(
        &self,
        writer: &AsyncArrowWriter<FileWriter>,
        incoming_rows: usize,
    ) -> bool {
        !writer.flushed_row_groups().is_empty()
            && incoming_rows > 0
            && self
                .options
                .target_file_size
                .is_some_and(|size| writer.bytes_written() >= size)
    }

    fn file_index(&self, index: usize) -> usize {
        self.first_file + index * self.file_stride
    }

    async fn cleanup(&mut self) -> Result<()> {
        if !self.options.cleanup_on_error {
            return Ok(());
        }
        let mut result = if let Some(writer) = self.writer.take() {
            writer.into_inner().abort().await
        } else {
            Ok(())
        };
        for index in 0..self.files_created {
            if let Err(error) = self.destination.remove_file(self.file_index(index)).await {
                result = Err(error);
            }
        }
        if let Err(error) = self.destination.remove_reservation().await {
            result = Err(error);
        }
        result
    }
}
