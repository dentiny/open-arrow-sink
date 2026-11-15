use open_arrow_sink_core::{Result, SessionCommitter, WriteSummary, async_trait};

pub(crate) struct ParquetCommitter;

#[async_trait]
impl SessionCommitter<u64> for ParquetCommitter {
    async fn commit(self: Box<Self>, outputs: Vec<u64>) -> Result<WriteSummary> {
        // Parquet has no metadata transaction; finish already closed each footer.
        Ok(WriteSummary {
            rows_written: outputs.into_iter().sum(),
        })
    }
    async fn abort(self: Box<Self>) -> Result<()> {
        Ok(())
    }
}
