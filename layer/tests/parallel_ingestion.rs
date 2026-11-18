use std::{
    num::NonZeroUsize,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering as AtomicOrdering},
    },
};

use futures::{FutureExt, stream};
use open_arrow_sink_core::{Ordering, Sink, SinkExt, WriteOptions};
use open_arrow_sink_layer::{InputSizeHint, ParallelIngestionLayer};
use open_arrow_sink_test_support::parallel_sink::{
    Config, Event, Failure, State, TestSink, release,
};
use open_arrow_sink_test_support::{int64_batch, unordered_options};

fn nz(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).unwrap()
}

#[rstest::rstest]
#[case(InputSizeHint::OutputBytes(201), 3, Some(3))]
#[case(InputSizeHint::SourceBytes(200), 2, Some(2))]
#[case(InputSizeHint::OutputBytes(1), 1, Some(1))]
#[case(InputSizeHint::SourceBytes(0), 1, Some(0))]
#[case(InputSizeHint::Unknown, 2, None)]
#[case(InputSizeHint::OutputBytes(u64::MAX), 4, Some(u64::MAX / 100 + 1))]
#[test]
fn plans_before_consumption_and_clamps_concurrency(
    #[case] hint: InputSizeHint,
    #[case] writers: usize,
    #[case] files: Option<u64>,
) {
    let policy = ParallelIngestionLayer::new(nz(100))
        .with_input_size(hint)
        .with_max_writers(nz(4))
        .with_fallback_writers(nz(2));
    let plan = policy.plan();
    assert_eq!(plan.input_size, hint);
    assert_eq!(plan.writers.get(), writers);
    assert_eq!(plan.estimated_files, files);
}

#[test]
fn unknown_fallback_is_also_capped() {
    assert_eq!(
        ParallelIngestionLayer::new(nz(1))
            .with_max_writers(nz(2))
            .with_fallback_writers(nz(9))
            .plan()
            .writers
            .get(),
        2
    );
    assert_eq!(
        ParallelIngestionLayer::new(nz(1))
            .with_input_size(InputSizeHint::OutputBytes(u64::MAX))
            .plan()
            .estimated_files,
        Some(u64::MAX)
    );
}

#[tokio::test]
async fn ingestion_starts_before_eof_and_applies_backpressure() {
    let state = State::shared(Config::default());
    let policy = ParallelIngestionLayer::new(nz(100))
        .with_input_size(InputSizeHint::SourceBytes(201))
        .with_max_writers(nz(2));
    let sink = TestSink(state.clone()).layer(policy);
    assert_eq!(sink.plan().writers.get(), 2);
    let mut writer = sink
        .open(
            (),
            int64_batch("id", vec![Some(1)]).schema(),
            unordered_options(),
        )
        .await
        .unwrap();
    assert_eq!(state.lock().unwrap().count(Event::Open), 2);
    let batch = int64_batch("id", vec![Some(1)]);
    writer.write(batch.clone()).await.unwrap();
    writer.write(batch.clone()).await.unwrap();
    assert_eq!(state.lock().unwrap().count(Event::WriteStarted), 2);
    {
        let next = writer.write(batch);
        futures::pin_mut!(next);
        assert!(next.as_mut().now_or_never().is_none());
        assert_eq!(state.lock().unwrap().count(Event::WriteStarted), 2);
        release(&state);
        next.await.unwrap();
    }
    assert_eq!(writer.commit().await.unwrap().rows_committed, 3);
    let state = state.lock().unwrap();
    assert_eq!(state.count(Event::CommitAttempt), 1);
    assert_eq!(state.count(Event::Finish), 2);
}

#[rstest::rstest]
#[case(InputSizeHint::SourceBytes(0))]
#[case(InputSizeHint::OutputBytes(u64::MAX))]
#[case(InputSizeHint::Unknown)]
#[tokio::test]
async fn estimates_never_truncate_or_replay_input(#[case] hint: InputSizeHint) {
    let state = State::shared(Config {
        gated: false,
        ..Default::default()
    });
    let sink =
        TestSink(state.clone()).layer(ParallelIngestionLayer::new(nz(100)).with_input_size(hint));
    let batch = int64_batch("id", vec![Some(1), Some(2)]);
    let summary = sink
        .write_all(
            (),
            batch.schema(),
            unordered_options(),
            stream::iter((0..5).map(|_| Ok(batch.clone()))),
        )
        .await
        .unwrap();
    assert_eq!(summary.rows_committed, 10);
    assert_eq!(state.lock().unwrap().count(Event::CommitAttempt), 1);
}

#[rstest::rstest]
#[case(WriteOptions::default().with_ordering(Ordering::PreserveInput))]
#[case(WriteOptions::default().with_target_file_size(0))]
#[case(WriteOptions::default().with_target_file_size(99))]
#[tokio::test]
async fn rejects_invalid_policy_before_opening_or_pulling(#[case] options: WriteOptions) {
    let state = State::shared(Config::default());
    let sink = TestSink(state.clone()).layer(ParallelIngestionLayer::new(nz(100)));
    let polls = Arc::new(AtomicUsize::new(0));
    let observed = polls.clone();
    let batch = int64_batch("id", vec![Some(1)]);
    let input = stream::poll_fn(move |_| {
        observed.fetch_add(1, AtomicOrdering::SeqCst);
        std::task::Poll::Ready(None)
    });
    assert!(
        sink.write_all((), batch.schema(), options, input)
            .await
            .is_err()
    );
    assert_eq!(polls.load(AtomicOrdering::SeqCst), 0);
    assert_eq!(state.lock().unwrap().count(Event::Open), 0);
}

#[tokio::test]
async fn write_failure_aborts_without_publishing() {
    let state = State::shared(Config {
        gated: false,
        ..Config::default().failing([Failure::Write])
    });
    let sink = TestSink(state.clone()).layer(
        ParallelIngestionLayer::new(nz(100)).with_input_size(InputSizeHint::SourceBytes(200)),
    );
    let batch = int64_batch("id", vec![Some(1)]);
    assert!(
        sink.write_all(
            (),
            batch.schema(),
            unordered_options(),
            stream::iter([Ok(batch)])
        )
        .await
        .is_err()
    );
    let state = state.lock().unwrap();
    assert_eq!(state.count(Event::CommitAttempt), 0);
    assert_eq!(state.count(Event::Abort), 2);
    assert_eq!(state.count(Event::SessionAbort), 1);
}

#[tokio::test]
async fn write_all_does_not_read_to_eof_before_starting_parallel_writes() {
    use futures::StreamExt;

    let state = State::shared(Config::default());
    let sink = TestSink(state.clone()).layer(
        ParallelIngestionLayer::new(nz(100)).with_input_size(InputSizeHint::SourceBytes(200)),
    );
    let pulled = Arc::new(AtomicUsize::new(0));
    let observed = pulled.clone();
    let batch = int64_batch("id", vec![Some(1)]);
    let input = stream::iter((0..20).map(|_| Ok(batch.clone()))).inspect(move |_| {
        observed.fetch_add(1, AtomicOrdering::SeqCst);
    });
    let ingestion = sink.write_all((), batch.schema(), unordered_options(), input);
    futures::pin_mut!(ingestion);
    assert!(ingestion.as_mut().now_or_never().is_none());
    assert_eq!(state.lock().unwrap().count(Event::WriteStarted), 2);
    // Two in-flight batches plus the current caller-held batch, without draining
    // the remaining 17 input batches while both writers are blocked.
    assert_eq!(pulled.load(AtomicOrdering::SeqCst), 3);
    release(&state);
    assert_eq!(ingestion.await.unwrap().rows_committed, 20);
    assert_eq!(pulled.load(AtomicOrdering::SeqCst), 20);
    assert_eq!(state.lock().unwrap().count(Event::CommitAttempt), 1);
}
