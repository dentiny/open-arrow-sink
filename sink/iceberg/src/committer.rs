use std::sync::Arc;

use iceberg::{
    Catalog,
    table::Table,
    transaction::{ApplyTransactionAction, Transaction},
};
use open_arrow_sink_core::{Error, Result, SessionCommitter, WriteSummary, async_trait};

pub(crate) struct IcebergCommitter {
    pub catalog: Arc<dyn Catalog>,
    pub table: Table,
}

#[async_trait]
impl SessionCommitter<(u64, Vec<iceberg::spec::DataFile>)> for IcebergCommitter {
    async fn commit(
        self: Box<Self>,
        outputs: Vec<(u64, Vec<iceberg::spec::DataFile>)>,
    ) -> Result<WriteSummary> {
        let mut rows_written = 0;
        let mut files = Vec::new();
        for (rows, output) in outputs {
            rows_written += rows;
            files.extend(output);
        }
        if !files.is_empty() {
            let tx = Transaction::new(&self.table);
            let tx = tx
                .fast_append()
                .add_data_files(files)
                .apply(tx)
                .map_err(Error::backend)?;
            // Retain data on error: catalog publication may have succeeded.
            tx.commit(self.catalog.as_ref())
                .await
                .map_err(Error::backend)?;
        }
        Ok(WriteSummary { rows_written })
    }
    async fn abort(self: Box<Self>) -> Result<()> {
        Ok(())
    }
}
