use iceberg::Result;
use iceberg::arrow::RecordBatchPartitionSplitter;
use iceberg::spec::{DataFile, PartitionSpecRef, SchemaRef};
use iceberg::writer::partitioning::{PartitioningWriter, fanout_writer::FanoutWriter};
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use open_arrow_sink_core::{RecordBatch, async_trait};

/// Adapt the SDK's keyed partition writer to its RecordBatch writer interface.
/// The schema/spec are pinned at open; the SDK computes keys and file metadata.
pub(crate) struct PartitionWriterAdapter<B: IcebergWriterBuilder> {
    splitter: RecordBatchPartitionSplitter,
    // The enclosing IcebergTableWriter gates write/finish with Poison. This
    // Option only moves the SDK writer into its consuming close operation.
    inner: Option<FanoutWriter<B>>,
}

impl<B: IcebergWriterBuilder> PartitionWriterAdapter<B> {
    pub(crate) fn try_new(builder: B, schema: SchemaRef, spec: PartitionSpecRef) -> Result<Self> {
        Ok(Self {
            splitter: RecordBatchPartitionSplitter::try_new_with_computed_values(schema, spec)?,
            inner: Some(FanoutWriter::new(builder)),
        })
    }
}

#[async_trait]
impl<B: IcebergWriterBuilder> IcebergWriter for PartitionWriterAdapter<B> {
    async fn write(&mut self, batch: RecordBatch) -> Result<()> {
        // Avoid opening writers for empty batches.
        if batch.num_rows() == 0 {
            return Ok(());
        }
        let batches = self.splitter.split(&batch)?;
        let inner = self
            .inner
            .as_mut()
            .expect("active table writer owns its partition writer");
        for (key, batch) in batches {
            inner.write(key, batch).await?;
        }
        Ok(())
    }

    async fn close(&mut self) -> Result<Vec<DataFile>> {
        let inner = self
            .inner
            .take()
            .expect("active table writer owns its partition writer");
        inner.close().await
    }
}
