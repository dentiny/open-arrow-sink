use open_arrow_sink_core::{Ordering, ParallelSink, SinkExt};
use open_arrow_sink_test_support::unordered_options as options;
use parallel_sink::{Config, Event, State, TestSink};
use std::{num::NonZeroUsize, sync::Arc};

#[path = "support/parallel_sink.rs"]
mod parallel_sink;

#[rstest::rstest]
#[case::preserving_sink_preserve_input(Ordering::PreserveInput, Ordering::PreserveInput)]
#[case::preserving_sink_unordered(Ordering::PreserveInput, Ordering::Unordered)]
#[case::unordered_sink_preserve_input(Ordering::Unordered, Ordering::PreserveInput)]
#[case::unordered_sink_unordered(Ordering::Unordered, Ordering::Unordered)]
#[tokio::test]
async fn ordering_layer_parallel_open_respects_inner_ordering_capability(
    #[case] capability_ordering: Ordering,
    #[case] requested: Ordering,
) {
    let state = State::shared(Config {
        ordering: capability_ordering,
        ..Default::default()
    });
    let sink = TestSink(state.clone()).layer(open_arrow_sink_layer::OrderingLayer::new(requested));
    let result = sink
        .open_writers(
            (),
            Arc::new(arrow_schema::Schema::empty()),
            options(),
            NonZeroUsize::MIN,
        )
        .await;
    let supported =
        capability_ordering == Ordering::PreserveInput || requested == Ordering::Unordered;
    assert_eq!(
        result.is_ok(),
        supported,
        "capability={capability_ordering:?}, requested={requested:?}"
    );
    assert_eq!(
        state.lock().unwrap().count(Event::Open),
        usize::from(supported)
    );
    if let Ok(session) = result {
        assert_eq!(state.lock().unwrap().requested_ordering(), Some(requested));
        for writer in session.writers {
            writer.abort().await.unwrap();
        }
        session.committer.abort().await.unwrap();
    }
}

#[tokio::test]
async fn ordering_override_replaces_the_callers_preserve_input_requirement() {
    let state = State::shared(Config {
        ordering: Ordering::Unordered,
        ..Default::default()
    });
    // An unordered backend would reject the caller's requirement without the layer.
    let caller_options = options().with_ordering(Ordering::PreserveInput);
    let session = TestSink(state.clone())
        .layer(open_arrow_sink_layer::OrderingLayer::new(
            Ordering::Unordered,
        ))
        .open_writers(
            (),
            Arc::new(arrow_schema::Schema::empty()),
            caller_options,
            NonZeroUsize::MIN,
        )
        .await
        .unwrap();
    assert_eq!(
        state.lock().unwrap().requested_ordering(),
        Some(Ordering::Unordered)
    );
    for writer in session.writers {
        writer.abort().await.unwrap();
    }
    session.committer.abort().await.unwrap();
}
