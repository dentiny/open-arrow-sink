use open_arrow_sink_core::CommitSummary;

#[test]
fn aggregate_counts_independently() {
    let summaries = [
        CommitSummary {
            rows_committed: 10,
            rows_skipped: 2,
            rows_failed_blobs: 5,
        },
        CommitSummary {
            rows_committed: 20,
            rows_skipped: 3,
            rows_failed_blobs: 7,
        },
    ];
    let expected = CommitSummary {
        rows_committed: 30,
        rows_skipped: 5,
        rows_failed_blobs: 12,
    };
    assert_eq!(summaries.into_iter().sum::<CommitSummary>(), expected);
    assert_eq!(summaries.iter().sum::<CommitSummary>(), expected);
    let mut total = summaries[0];
    total += summaries[1];
    assert_eq!(total, expected);
}

#[test]
fn empty_sum_has_zero_counts() {
    assert_eq!(
        std::iter::empty::<CommitSummary>().sum::<CommitSummary>(),
        CommitSummary::default(),
    );
}

#[test]
fn aggregation_saturates_each_count() {
    let mut total = CommitSummary {
        rows_committed: u64::MAX - 1,
        rows_skipped: u64::MAX - 2,
        rows_failed_blobs: u64::MAX,
    };
    let increment = CommitSummary {
        rows_committed: 2,
        rows_skipped: 3,
        rows_failed_blobs: 1,
    };
    let summed: CommitSummary = [total, increment].into_iter().sum();
    total += increment;
    assert_eq!(summed, total);
    assert_eq!(
        total,
        CommitSummary {
            rows_committed: u64::MAX,
            rows_skipped: u64::MAX,
            rows_failed_blobs: u64::MAX,
        },
    );
}
