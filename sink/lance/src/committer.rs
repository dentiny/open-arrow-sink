use lance::dataset::transaction::Transaction;
use open_arrow_sink_core::{
    Error, Result, SessionCommitter, WriteOptions, WriteSummary, async_trait,
};

use crate::{LanceFragments, index::build_indexes, storage::NativeStorage};

pub(crate) struct LanceCommitter {
    pub destination: NativeStorage,
    pub options: WriteOptions,
}

#[async_trait]
impl SessionCommitter<LanceFragments> for LanceCommitter {
    async fn commit(self: Box<Self>, outputs: Vec<LanceFragments>) -> Result<WriteSummary> {
        let mut rows_written = 0;
        let mut combined: Option<Transaction> = None;
        for LanceFragments {
            rows_written: rows,
            mut transaction,
        } in outputs
        {
            rows_written += rows;
            if let Some(target) = &mut combined {
                crate::validation::validate_transaction_compatibility(target, &transaction)?;
                let fragments = crate::validation::write_fragments(&mut target.operation)?;
                let incoming = crate::validation::write_fragments(&mut transaction.operation)?;
                fragments.append(incoming);
            } else {
                combined = Some(transaction);
            }
        }
        let transaction = crate::validation::validate_session_transaction(combined)?;
        // One manifest publication assigns fragment IDs across all workers.
        // Retain files on failure: publication may already have succeeded.
        let mut builder = self.destination.commit_builder();
        if let Some(version) =
            crate::validation::parse_storage_version(self.options.storage_version.as_deref())?
        {
            // Fragment writes and manifest publication must use the same format.
            // Lance preserves the existing manifest default for append operations.
            builder = builder.with_storage_format(version);
        }
        let mut dataset = builder.execute(transaction).await.map_err(Error::backend)?;
        let summary = WriteSummary { rows_written };
        build_indexes(&mut dataset, &self.options.indexes, &summary)
            .await
            .map_err(Error::backend)?;
        Ok(summary)
    }
    async fn abort(self: Box<Self>) -> Result<()> {
        if self.options.cleanup_on_error {
            self.destination.cleanup().await?;
        }
        Ok(())
    }
}
