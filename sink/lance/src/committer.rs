use std::sync::Arc;

use lance::dataset::transaction::{Operation, Transaction};
use open_arrow_sink_core::{Error, IndexSpec, Result, SessionCommitter, async_trait};

use crate::{
    LanceFragments, destination::LanceDestination, error, indexed_commit::IndexedCommitHandler,
};

pub(crate) struct LanceCommitter {
    pub destination: LanceDestination,
    pub indexes: Vec<IndexSpec>,
    pub storage_version: Option<lance_file::version::LanceFileVersion>,
}

#[async_trait]
impl SessionCommitter<LanceFragments> for LanceCommitter {
    async fn commit(self: Box<Self>, outputs: Vec<LanceFragments>) -> Result<()> {
        let transaction = merge_transactions(outputs)?;
        // One manifest publication assigns fragment IDs across all workers.
        // Retain files on failure: publication may already have succeeded.
        let mut builder = self.destination.commit_builder();
        if let Some(version) = self.storage_version {
            // Fragment writes and manifest publication must use the same format.
            // Lance preserves the existing manifest default for append operations.
            builder = builder.with_storage_format(version);
        }
        if !self.indexes.is_empty() {
            // Lance caches the operation's original index list after commit.
            // Keep that bookkeeping cache out of subsequent reader sessions:
            // the published manifest also contains indexes built by the handler.
            builder = builder
                .with_session(self.destination.isolated_session())
                .with_commit_handler(Arc::new(IndexedCommitHandler {
                    destination: self.destination.clone(),
                    indexes: self.indexes,
                }));
        }
        builder.execute(transaction).await.map_err(Error::backend)?;
        Ok(())
    }
    async fn abort(self: Box<Self>) -> Result<()> {
        Ok(())
    }
}

/// Combine worker fragments without publishing or discarding native transaction metadata.
fn merge_transactions(outputs: Vec<LanceFragments>) -> Result<Transaction> {
    let mut outputs = outputs.into_iter();
    let LanceFragments { mut transaction } =
        outputs.next().ok_or_else(error::empty_writer_session)?;
    for LanceFragments {
        transaction: incoming,
    } in outputs
    {
        if transaction.read_version != incoming.read_version {
            return Err(error::different_dataset_versions());
        }
        match (&mut transaction.operation, incoming.operation) {
            (
                Operation::Overwrite { fragments, .. },
                Operation::Overwrite {
                    fragments: mut other,
                    ..
                },
            )
            | (
                Operation::Append { fragments },
                Operation::Append {
                    fragments: mut other,
                },
            ) => {
                fragments.append(&mut other);
            }
            _ => return Err(error::invalid_create_fragments()),
        }
    }
    Ok(transaction)
}
