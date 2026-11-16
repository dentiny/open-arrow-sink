use open_arrow_sink_core::{Result, SchemaRef, SessionCommitter, WriteSummary, async_trait};
use parquet::file::properties::WriterProperties;
use std::sync::Arc;

pub(crate) struct ParquetCommitter {
    pub destination: Arc<crate::destination::Destination>,
    pub schema: SchemaRef,
    pub properties: WriterProperties,
}

#[async_trait]
impl SessionCommitter<u64> for ParquetCommitter {
    async fn commit(self: Box<Self>, outputs: Vec<u64>) -> Result<WriteSummary> {
        let rows_written = outputs.into_iter().sum();
        // Idle workers never open a file. A newly created/replaced empty dataset
        // still needs one footer to make its schema readable; empty Append does not.
        if rows_written == 0
            && self.destination.needs_empty_file()
            && let Err(error) = self
                .destination
                .write_empty_file(self.schema, self.properties)
                .await
        {
            let _ = self.destination.cleanup_reservation().await;
            return Err(error);
        }
        self.destination.complete_reservation().await?;
        // Parquet has no metadata transaction; data writers already closed footers.
        self.destination.publish().await?;
        Ok(WriteSummary { rows_written })
    }
    async fn abort(self: Box<Self>) -> Result<()> {
        self.destination.cleanup_reservation().await
    }
}
