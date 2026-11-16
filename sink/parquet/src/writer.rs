use open_arrow_sink_core::{
    DataWriter, Poison, RecordBatch, Result, SchemaRef, WriteOptions, async_trait, validation,
};
use parquet::{arrow::AsyncArrowWriter, file::properties::WriterProperties};
use std::sync::Arc;

use crate::{
    destination::Destination,
    error::{backend, closed},
    file_writer::FileWriter,
};

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
                .map_err(backend)?;
            self.writer = None;
        }
        if self.writer.is_none() {
            let index = self.file_index(self.files_created);
            self.destination.reserve_file(index).await?;
            // Retain ownership before opening/awaiting the upload so abort can
            // remove this part after cancellation, without deleting collisions.
            self.files_created += 1;
            let writer = self
                .destination
                .open_reserved_writer(index, self.schema.clone(), self.properties.clone())
                .await?;
            self.writer = Some(writer);
        }
        let writer = self.writer.as_mut().ok_or_else(closed)?;
        writer.write(&batch).await.map_err(backend)?;
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
            writer.flush().await.map_err(backend)?;
        }
        self.rows_written += batch.num_rows() as u64;
        self.state.complete();
        Ok(())
    }

    async fn finish(&mut self) -> Result<u64> {
        self.state.begin()?;
        if let Some(writer) = self.writer.as_mut() {
            writer.finish().await.map_err(backend)?;
            self.writer = None;
        }
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
        options: WriteOptions,
        properties: WriterProperties,
        first_file: usize,
        file_stride: usize,
    ) -> Self {
        Self {
            destination,
            schema,
            writer: None,
            state: Poison::default(),
            options,
            properties,
            rows_written: 0,
            files_created: 0,
            first_file,
            file_stride,
        }
    }

    fn should_roll_file(&self, writer: &AsyncArrowWriter<FileWriter>) -> bool {
        !writer.flushed_row_groups().is_empty()
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
