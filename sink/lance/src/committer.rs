use lance::dataset::transaction::{Operation, Transaction};
use open_arrow_sink_core::{
    Error, Result, SessionCommitter, WriteOptions, WriteSummary, async_trait,
};

use crate::{error::backend, index::build_indexes, storage::NativeStorage};

type Output = (u64, Transaction);

pub(crate) struct LanceCommitter {
    pub destination: NativeStorage,
    pub options: WriteOptions,
}

#[async_trait]
impl SessionCommitter<Output> for LanceCommitter {
    async fn commit(self: Box<Self>, outputs: Vec<Output>) -> Result<WriteSummary> {
        let mut rows_written = 0;
        let mut combined: Option<Transaction> = None;
        for (rows, transaction) in outputs {
            rows_written += rows;
            if let Some(target) = &mut combined {
                match (&mut target.operation, transaction.operation) {
                    (
                        Operation::Overwrite { fragments, .. },
                        Operation::Overwrite {
                            fragments: incoming,
                            ..
                        },
                    ) => fragments.extend(incoming),
                    _ => {
                        return Err(Error::Unsupported(
                            "expected uncommitted Lance create fragments".into(),
                        ));
                    }
                }
            } else {
                combined = Some(transaction);
            }
        }
        let transaction =
            combined.ok_or_else(|| Error::Unsupported("empty Lance writer session".into()))?;
        // One manifest publication assigns fragment IDs across all workers.
        // Retain files on failure: publication may already have succeeded.
        let mut dataset = self
            .destination
            .commit_builder()
            .execute(transaction)
            .await
            .map_err(backend)?;
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
