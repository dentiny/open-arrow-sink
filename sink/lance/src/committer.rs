use std::path::PathBuf;

use lance::dataset::{
    CommitBuilder,
    transaction::{Operation, Transaction},
};
use open_arrow_sink_core::{
    Error, Result, SessionCommitter, WriteOptions, WriteSummary, async_trait,
};

use crate::{error::backend, index::build_indexes};

type Output = (u64, Transaction);

pub(crate) struct LanceCommitter {
    pub path: Option<PathBuf>,
    pub uri: String,
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
                if target.read_version != transaction.read_version {
                    return Err(Error::Unsupported(
                        "Lance writers observed different dataset versions".into(),
                    ));
                }
                match (&mut target.operation, transaction.operation) {
                    (
                        Operation::Overwrite { fragments, .. },
                        Operation::Overwrite {
                            fragments: incoming,
                            ..
                        },
                    ) => fragments.extend(incoming),
                    (
                        Operation::Append { fragments },
                        Operation::Append {
                            fragments: incoming,
                        },
                    ) => fragments.extend(incoming),
                    _ => {
                        return Err(Error::Unsupported(
                            "expected compatible uncommitted Lance write fragments".into(),
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
        let mut dataset = CommitBuilder::new(self.uri.as_str())
            .execute(transaction)
            .await
            .map_err(backend)?;
        build_indexes(&mut dataset, &self.options.indexes)
            .await
            .map_err(backend)?;
        Ok(WriteSummary { rows_written })
    }
    async fn abort(self: Box<Self>) -> Result<()> {
        if self.options.cleanup_on_error
            && let Some(path) = &self.path
        {
            tokio::fs::remove_dir_all(path).await.map_err(backend)?;
        }
        Ok(())
    }
}
