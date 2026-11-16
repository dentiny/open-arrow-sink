use open_arrow_sink_core::{Result, SchemaRef, SessionCommitter, async_trait};
use parquet::file::properties::WriterProperties;
use std::sync::Arc;

pub(crate) struct ParquetCommitter {
    pub destination: Arc<crate::destination::Destination>,
    pub schema: SchemaRef,
    pub properties: WriterProperties,
}

#[async_trait]
impl SessionCommitter<u64> for ParquetCommitter {
    async fn commit(self: Box<Self>, outputs: Vec<u64>) -> Result<()> {
        let rows_written: u64 = outputs.into_iter().sum();
        // Idle workers never open a file. A newly created empty dataset
        // still needs one footer to make its schema readable; empty Append does not.
        if rows_written == 0 && self.destination.needs_empty_file() {
            self.destination
                .write_empty_file(self.schema, self.properties)
                .await?;
        }
        // Parquet has no metadata transaction; data writers already closed footers.
        Ok(())
    }
    async fn abort(self: Box<Self>) -> Result<()> {
        Ok(())
    }
}
