use open_arrow_sink_core::{
    DataWriter, RecordBatch, Result, SchemaRef, WriteOptions, WriteSummary, Writer, async_trait,
    validation,
};
use parquet::{arrow::AsyncArrowWriter, file::properties::WriterProperties};

use crate::{
    destination::{Destination, FileWriter},
    error::{backend, closed},
};

pub(crate) struct ParquetDirectoryWriter {
    destination: Destination,
    schema: SchemaRef,
    writer: Option<AsyncArrowWriter<FileWriter>>,
    options: WriteOptions,
    active: bool,
    properties: WriterProperties,
    rows_written: u64,
    files_created: usize,
    first_file: usize,
    file_stride: usize,
}

#[async_trait]
impl Writer for ParquetDirectoryWriter {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    async fn write(&mut self, batch: RecordBatch) -> Result<()> {
        if !self.active {
            return Err(closed());
        }
        // A failed or cancelled write is terminal, but retain the upload for abort.
        self.active = false;
        validation::validate_batch(&self.schema, &batch)?;
        if self.should_roll_file(self.writer.as_ref().ok_or_else(closed)?, batch.num_rows()) {
            self.writer
                .as_mut()
                .unwrap()
                .finish()
                .await
                .map_err(backend)?;
            self.writer = None;
            let file = self
                .destination
                .open_file(
                    self.file_index(self.files_created),
                    self.options.cleanup_on_error,
                )
                .await?;
            self.files_created += 1;
            self.writer = Some(
                AsyncArrowWriter::try_new(file, self.schema.clone(), Some(self.properties.clone()))
                    .map_err(backend)?,
            );
        }
        let writer = self.writer.as_mut().ok_or_else(closed)?;
        writer.write(&batch).await.map_err(backend)?;
        writer.flush().await.map_err(backend)?;
        self.rows_written += batch.num_rows() as u64;
        self.active = true;
        Ok(())
    }

    async fn commit(mut self: Box<Self>) -> Result<WriteSummary> {
        if !self.active {
            let _ = self.cleanup().await;
            return Err(closed());
        }
        self.active = false;
        if let Err(error) = self.writer.as_mut().ok_or_else(closed)?.finish().await {
            let _ = self.cleanup().await;
            return Err(backend(error));
        }
        self.writer = None;
        self.destination.complete_reservation().await?;
        Ok(WriteSummary {
            rows_written: self.rows_written,
        })
    }

    async fn abort(mut self: Box<Self>) -> Result<()> {
        self.active = false;
        self.cleanup().await
    }
}

impl ParquetDirectoryWriter {
    pub(crate) fn new(
        destination: Destination,
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
            active: true,
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
        let upload_result = if let Some(writer) = self.writer.take() {
            writer.into_inner().abort().await
        } else {
            Ok(())
        };
        let mut result = upload_result;
        for index in 0..self.files_created {
            if let Err(error) = self.destination.remove_file(self.file_index(index)).await {
                result = Err(error);
            }
        }
        if let Err(error) = self
            .destination
            .remove_reservation(self.file_stride > 1)
            .await
        {
            result = Err(error);
        }
        result
    }
}

#[async_trait]
impl DataWriter for ParquetDirectoryWriter {
    type Output = u64;
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }
    async fn write(&mut self, batch: RecordBatch) -> Result<()> {
        Writer::write(self, batch).await
    }
    async fn finish(&mut self) -> Result<u64> {
        if !self.active {
            return Err(closed());
        }
        self.active = false;
        self.writer
            .as_mut()
            .ok_or_else(closed)?
            .finish()
            .await
            .map_err(backend)?;
        self.writer = None;
        self.destination.complete_reservation().await?;
        Ok(self.rows_written)
    }
    async fn abort(self: Box<Self>) -> Result<()> {
        Writer::abort(self).await
    }
}
