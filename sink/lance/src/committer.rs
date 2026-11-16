use crate::config::SessionOptions;
use lance::dataset::transaction::Transaction;
use open_arrow_sink_core::{Result, SessionCommitter, WriteSummary, async_trait};

use crate::{destination::LanceDestination, error::backend, index::build_indexes};

type Output = (u64, Transaction);

pub(crate) struct LanceCommitter {
    pub destination: LanceDestination,
    pub options: SessionOptions,
}

#[async_trait]
impl SessionCommitter<Output> for LanceCommitter {
    async fn commit(self: Box<Self>, outputs: Vec<Output>) -> Result<WriteSummary> {
        let mut rows_written = 0;
        let mut combined: Option<Transaction> = None;
        for (rows, mut transaction) in outputs {
            rows_written += rows;
            if let Some(target) = &mut combined {
                validate_transaction_compatibility(target, &transaction)?;
                let fragments = write_fragments(&mut target.operation)?;
                let incoming = write_fragments(&mut transaction.operation)?;
                fragments.append(incoming);
            } else {
                combined = Some(transaction);
            }
        }
        let transaction = combined.ok_or_else(crate::error::empty_writer_session)?;
        // One manifest publication assigns fragment IDs across all workers.
        // Retain files on failure: publication may already have succeeded.
        let mut builder = self.destination.commit_builder();
        if let Some(version) = self.options.storage_version {
            // Fragment writes and manifest publication must use the same format.
            // Lance preserves the existing manifest default for append operations.
            builder = builder.with_storage_format(version);
        }
        let mut dataset = builder.execute(transaction).await.map_err(backend)?;
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

fn validate_transaction_compatibility(
    target: &lance::dataset::transaction::Transaction,
    incoming: &lance::dataset::transaction::Transaction,
) -> Result<()> {
    use lance::dataset::transaction::Operation;
    if target.read_version != incoming.read_version {
        return Err(crate::error::different_dataset_versions());
    }
    if !matches!(
        (&target.operation, &incoming.operation),
        (Operation::Overwrite { .. }, Operation::Overwrite { .. })
            | (Operation::Append { .. }, Operation::Append { .. })
    ) {
        return Err(crate::error::invalid_create_fragments());
    }
    Ok(())
}

fn write_fragments(
    operation: &mut lance::dataset::transaction::Operation,
) -> Result<&mut Vec<lance_table::format::Fragment>> {
    match operation {
        lance::dataset::transaction::Operation::Overwrite { fragments, .. }
        | lance::dataset::transaction::Operation::Append { fragments } => Ok(fragments),
        _ => Err(crate::error::invalid_create_fragments()),
    }
}
