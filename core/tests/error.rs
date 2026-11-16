use open_arrow_sink_core::{Error, IndexCommitError, Result, WriteSummary};

#[test]
fn index_failure_exposes_the_committed_summary() {
    fn commit() -> Result<()> {
        let indexed: std::result::Result<(), IndexCommitError> = Err(IndexCommitError::new(
            "index worker failed",
            WriteSummary { rows_written: 42 },
        ));
        indexed?;
        Ok(())
    }

    match commit() {
        Err(Error::IndexCommit(error)) => {
            assert_eq!(error.summary.rows_written, 42);
            assert_eq!(error.message, "index worker failed");
        }
        result => panic!("expected a committed-data failure, got {result:?}"),
    }
}
