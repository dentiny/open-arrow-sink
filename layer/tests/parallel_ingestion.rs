// Policy-specific coverage lives here; parallel_writer.rs owns the shared
// dispatch, ordering, cancellation, and finalization contracts.
use std::{
    num::NonZeroUsize,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering as AtomicOrdering},
    },
};

use futures::{FutureExt, stream};
use open_arrow_sink_core::{Sink, SinkExt};
use open_arrow_sink_layer::{InputSizeHint, ParallelIngestionLayer};
use open_arrow_sink_test_support::parallel_sink::{
    Config, Event, State, TestSink, release, wait_for_write_event,
};
use open_arrow_sink_test_support::{int64_batch, unordered_options};

fn nz(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).unwrap()
}

// Exercise rounding, zero vs unknown, fallback capping, and overflow-safe
// ceiling division without opening a destination or inspecting record data.
#[rstest::rstest]
#[case(InputSizeHint::OutputBytes(201), 2, 3, Some(3))]
#[case(InputSizeHint::SourceBytes(0), 2, 1, Some(0))]
#[case(InputSizeHint::Unknown, 2, 2, None)]
#[case(InputSizeHint::Unknown, 9, 4, None)]
#[case(InputSizeHint::OutputBytes(u64::MAX), 2, 4, Some(u64::MAX / 100 + 1))]
#[test]
fn plans_before_consumption_and_clamps_concurrency(
    #[case] hint: InputSizeHint,
    #[case] fallback: usize,
    #[case] writers: usize,
    #[case] files: Option<u64>,
) {
    let policy = ParallelIngestionLayer::new(nz(100))
        .with_input_size(hint)
        .with_max_writers(nz(4))
        .with_fallback_writers(nz(fallback));
    let plan = policy.plan();
    assert_eq!(plan.input_size, hint);
    assert_eq!(plan.writers.get(), writers);
    assert_eq!(plan.estimated_files, files);
}

#[tokio::test]
async fn conflicting_target_is_rejected_before_opening_or_pulling() {
    let state = State::shared(Config::default());
    let sink = TestSink(state.clone()).layer(ParallelIngestionLayer::new(nz(100)));
    let batch = int64_batch("id", vec![Some(1)]);
    // Conflicting targets must fail before source consumption or destination I/O.
    let input = stream::poll_fn(|_| panic!("input polled before validating target"));
    assert!(
        sink.write_all(
            (),
            batch.schema(),
            unordered_options().with_target_file_size(99),
            input,
        )
        .await
        .is_err()
    );
    assert_eq!(state.lock().unwrap().count(Event::Open), 0);
}

// The zero hint must still drain real input. Blocking every writer also makes
// this test catch eager input collection before EOF and missing backpressure.
#[rstest::rstest]
#[case(InputSizeHint::SourceBytes(201), 2)]
#[case(InputSizeHint::SourceBytes(0), 1)]
#[tokio::test]
async fn write_all_streams_with_backpressure_even_when_size_is_underestimated(
    #[case] hint: InputSizeHint,
    #[case] writers: usize,
) {
    use futures::StreamExt;

    let state = State::shared(Config::default());
    let sink = TestSink(state.clone()).layer(
        ParallelIngestionLayer::new(nz(100))
            .with_input_size(hint)
            .with_max_writers(nz(2)),
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
    assert_eq!(state.lock().unwrap().count(Event::Open), writers);
    wait_for_write_event(&state, Event::WriteStarted, writers).await;
    // In-flight batches plus one caller-held batch; the input has not reached EOF.
    assert_eq!(pulled.load(AtomicOrdering::SeqCst), writers + 1);
    release(&state);
    assert_eq!(ingestion.await.unwrap().rows_committed, 20);
    assert_eq!(pulled.load(AtomicOrdering::SeqCst), 20);
    assert_eq!(state.lock().unwrap().count(Event::CommitAttempt), 1);
}

#[rstest::rstest]
#[case(None)]
#[case(Some(123))]
#[tokio::test]
async fn fixed_mode_preserves_caller_target_and_explicit_concurrency(
    #[case] target: Option<usize>,
) {
    use open_arrow_sink_layer::OptionsLayer;
    use std::sync::Mutex;

    let state = State::shared(Config {
        gated: false,
        ..Default::default()
    });
    let captured = Arc::new(Mutex::new(Vec::new()));
    let observed = captured.clone();
    // Observe options downstream of ingestion: fixed mode must preserve both
    // an explicit target and None, which delegates to the backend default.
    let sink = TestSink(state.clone())
        .layer(OptionsLayer::new(move |options| {
            observed.lock().unwrap().push(options.target_file_size);
        }))
        .layer(ParallelIngestionLayer::fixed(nz(3)));
    let plan = sink.plan();
    assert_eq!(plan.writers.get(), 3);
    assert_eq!(plan.target_file_size, None);
    assert_eq!(plan.estimated_files, None);
    let mut options = unordered_options();
    options.target_file_size = target;
    let batch = int64_batch("id", vec![Some(1)]);
    let writer = sink.open((), batch.schema(), options).await.unwrap();
    assert_eq!(*captured.lock().unwrap(), vec![target]);
    assert_eq!(state.lock().unwrap().count(Event::Open), 3);
    writer.commit().await.unwrap();
    assert_eq!(state.lock().unwrap().count(Event::CommitAttempt), 1);
}
