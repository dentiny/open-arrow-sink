use lance::dataset::transaction::Transaction;
use open_arrow_sink_core::{Result, SessionCommitter, WriteOptions, WriteSummary, async_trait};

use crate::{error::backend, index::build_indexes, storage::NativeStorage};

type Output = (u64, Transaction);

pub(crate) struct LanceCommitter {
    pub destination: NativeStorage,
    pub options: WriteOptions,
}

#[async_trait]
impl SessionCommitter<Output> for LanceCommitter {
    fn pre_commit_outcome(&self) -> open_arrow_sink_core::PublicationOutcome {
        open_arrow_sink_core::PublicationOutcome::NotPublished
    }
    async fn commit(self: Box<Self>, outputs: Vec<Output>) -> Result<WriteSummary> {
        let mut rows_written = 0;
        let mut combined: Option<Transaction> = None;
        for (rows, mut transaction) in outputs {
            rows_written += rows;
            if let Some(target) = &mut combined {
                crate::validation::validate_transaction_compatibility(target, &transaction)
                    .map_err(not_published)?;
                let fragments = crate::validation::write_fragments(&mut target.operation)
                    .map_err(not_published)?;
                let incoming = crate::validation::write_fragments(&mut transaction.operation)
                    .map_err(not_published)?;
                fragments.append(incoming);
            } else {
                combined = Some(transaction);
            }
        }
        let transaction =
            crate::validation::validate_session_transaction(combined).map_err(not_published)?;
        // One manifest publication assigns fragment IDs across all workers.
        // Retain files on failure: publication may already have succeeded.
        let mut builder = self.destination.commit_builder();
        if let Some(version) =
            crate::validation::parse_storage_version(self.options.storage_version.as_deref())
                .map_err(not_published)?
        {
            // Fragment writes and manifest publication must use the same format.
            // Lance preserves the existing manifest default for append operations.
            builder = builder.with_storage_format(version);
        }
        let mut dataset = builder.execute(transaction).await.map_err(|error| {
            backend(error)
                .with_publication_outcome(open_arrow_sink_core::PublicationOutcome::Unknown)
        })?;
        build_indexes(&mut dataset, &self.options.indexes)
            .await
            .map_err(backend)?;
        Ok(WriteSummary { rows_written })
    }
    async fn abort(self: Box<Self>) -> Result<()> {
        if self.options.cleanup_on_error {
            self.destination.cleanup().await?;
        }
        Ok(())
    }
}

fn not_published(error: open_arrow_sink_core::Error) -> open_arrow_sink_core::Error {
    error.with_publication_outcome(open_arrow_sink_core::PublicationOutcome::NotPublished)
}
