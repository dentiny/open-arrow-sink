use std::{io, path::PathBuf};

use open_arrow_sink_core::{
    DataWriter, RecordBatch, Result, SchemaRef, WriteOptions, WriteSummary, Writer, async_trait,
    validation,
};
use parquet::arrow::AsyncArrowWriter;
use tokio::fs::{self, File, OpenOptions};

use crate::error::{backend, closed};

pub(crate) struct ParquetDirectoryWriter {
    directory: PathBuf,
    schema: SchemaRef,
    writer: Option<AsyncArrowWriter<File>>,
    options: WriteOptions,
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
        // Taking ownership also makes a failed or cancelled write terminal.
        let mut writer = self.writer.take().ok_or_else(closed)?;
        validation::validate_batch(&self.schema, &batch)?;
        if self.should_roll_file(&writer, batch.num_rows()) {
            writer.close().await.map_err(backend)?;
            let file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(self.file_path(self.files_created))
                .await
                .map_err(backend)?;
            self.files_created += 1;
            writer = AsyncArrowWriter::try_new(file, self.schema.clone(), None).map_err(backend)?;
        }
        writer.write(&batch).await.map_err(backend)?;
        // Flush each batch rather than retaining the complete input in memory.
        writer.flush().await.map_err(backend)?;
        self.rows_written += batch.num_rows() as u64;
        self.writer = Some(writer);
        Ok(())
    }

    async fn commit(mut self: Box<Self>) -> Result<WriteSummary> {
        let Some(writer) = self.writer.take() else {
            let _ = self.cleanup().await;
            return Err(closed());
        };
        if let Err(error) = writer.close().await {
            let _ = self.cleanup().await;
            return Err(backend(error));
        }
        Ok(WriteSummary {
            rows_written: self.rows_written,
        })
    }

    async fn abort(mut self: Box<Self>) -> Result<()> {
        drop(self.writer.take());
        self.cleanup().await.map_err(backend)
    }
}

impl ParquetDirectoryWriter {
    pub(crate) fn new(
        directory: PathBuf,
        schema: SchemaRef,
        writer: AsyncArrowWriter<File>,
        options: WriteOptions,
        first_file: usize,
        file_stride: usize,
    ) -> Self {
        Self {
            directory,
            schema,
            writer: Some(writer),
            options,
            rows_written: 0,
            files_created: 1,
            first_file,
            file_stride,
        }
    }

    fn should_roll_file(&self, writer: &AsyncArrowWriter<File>, incoming_rows: usize) -> bool {
        !writer.flushed_row_groups().is_empty()
            && incoming_rows > 0
            && self
                .options
                .target_file_size
                .is_some_and(|size| writer.bytes_written() >= size)
    }

    fn file_path(&self, index: usize) -> PathBuf {
        let index = self.first_file + index * self.file_stride;
        self.directory.join(format!("part-{index:05}.parquet"))
    }

    async fn cleanup(&mut self) -> io::Result<()> {
        if !self.options.cleanup_on_error {
            return Ok(());
        }
        for index in 0..self.files_created {
            match fs::remove_file(self.file_path(index)).await {
                Ok(()) => {}
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => return Err(error),
            }
        }
        // Never recursively delete a directory: other files may have been added.
        match fs::remove_dir(&self.directory).await {
            // Other shards or unrelated files can still own this directory.
            Err(error)
                if self.file_stride > 1
                    && matches!(
                        error.kind(),
                        io::ErrorKind::DirectoryNotEmpty | io::ErrorKind::NotFound
                    ) =>
            {
                Ok(())
            }
            result => result,
        }
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
        let writer = self.writer.take().ok_or_else(closed)?;
        writer.close().await.map_err(backend)?;
        Ok(self.rows_written)
    }
    async fn abort(self: Box<Self>) -> Result<()> {
        Writer::abort(self).await
    }
}
