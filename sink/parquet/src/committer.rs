use open_arrow_sink_core::{Result, SessionCommitter, WriteSummary, async_trait};
use std::sync::Arc;

use crate::destination::Destination;

pub(crate) struct ParquetCommitter {
    pub(crate) destination: Arc<Destination>,
    pub(crate) cleanup_on_error: bool,
}

#[async_trait]
impl SessionCommitter<u64> for ParquetCommitter {
    async fn commit(self: Box<Self>, outputs: Vec<u64>) -> Result<WriteSummary> {
        // Parquet has no metadata transaction; finish already closed each footer.
        Ok(WriteSummary {
            rows_written: outputs.into_iter().sum(),
        })
    }
    async fn abort(self: Box<Self>) -> Result<()> {
        if !self.cleanup_on_error {
            return Ok(());
        }
        let directory = self.destination.directory();
        let mut result = Ok(());
        for path in self.destination.claimed_files() {
            if let Err(error) = directory.storage.delete(&path).await {
                result = Err(error);
            }
        }
        if let Destination::Owned(directory) = self.destination.as_ref()
            && let Err(error) = directory
                .storage
                .remove_empty_directory(&directory.path)
                .await
        {
            result = Err(error);
        }
        result
    }
}
