#[path = "support/parallel_sink.rs"]
mod parallel_sink;
use futures::FutureExt;
use open_arrow_sink_core::{Error, Ordering, RecordBatch, Sink, SinkExt, WriteOptions};
use open_arrow_sink_layer::UnorderedWriteLayer;
use open_arrow_sink_test_support::{int64_batch, unordered_options as options};
use parallel_sink::{Config, Event, Failure, SessionDefect, State, TestSink, release};
use std::{
    num::NonZeroUsize,
    sync::{Arc, Mutex},
};
fn batch() -> RecordBatch {
    int64_batch("id", vec![Some(1), Some(2), Some(3)])
}
fn sink(state: &Arc<Mutex<State>>) -> impl Sink<Request = ()> {
    TestSink(state.clone()).layer(UnorderedWriteLayer::new(NonZeroUsize::new(2).unwrap()))
}

#[tokio::test]
async fn preopens_writers_and_bounds_concurrent_batches() {
    let state = Arc::new(Mutex::new(State::default()));
    let batch = batch();
    let mut writer = sink(&state)
        .open((), batch.schema(), options())
        .await
        .unwrap();
    assert_eq!(
        state.lock().unwrap().count(|e| matches!(e, Event::Open(_))),
        2
    );
    writer.write(batch.clone()).await.unwrap();
    writer.write(batch.clone()).await.unwrap();
    assert_eq!(
        state
            .lock()
            .unwrap()
            .count(|e| matches!(e, Event::WriteStarted(_))),
        2
    );
    assert_eq!(
        state
            .lock()
            .unwrap()
            .count(|e| matches!(e, Event::WriteCompleted(_))),
        0
    );
    {
        let third = writer.write(batch);
        futures::pin_mut!(third);
        assert!(third.as_mut().now_or_never().is_none());
        assert_eq!(
            state
                .lock()
                .unwrap()
                .count(|e| matches!(e, Event::WriteStarted(_))),
            2
        );
        release(&state);
        third.await.unwrap();
    }
    assert_eq!(writer.commit().await.unwrap().rows_written, 9);
    let state = state.lock().unwrap();
    assert_eq!(state.count(|e| matches!(e, Event::Commit { .. })), 1);
    assert_eq!(state.committed_rows(), Some(9));
    assert_eq!(state.count(|e| matches!(e, Event::WriteCompleted(_))), 3);
    let publication = state
        .events
        .iter()
        .position(|event| matches!(event, Event::CommitAttempt))
        .unwrap();
    assert_eq!(
        state.events[..publication]
            .iter()
            .filter(|event| matches!(event, Event::Finish(_)))
            .count(),
        2
    );
    assert_eq!(state.events.last(), Some(&Event::Commit { rows: 9 }));
}

#[rstest::rstest]
#[case::preserve_input(options().with_ordering(Ordering::PreserveInput))]
#[case::zero_target(options().with_target_file_size(0))]
#[tokio::test]
async fn rejects_preserve_order_and_invalid_size_before_open(#[case] options: WriteOptions) {
    let state = Arc::new(Mutex::new(State::default()));
    assert!(
        sink(&state)
            .open((), batch().schema(), options)
            .await
            .is_err()
    );
    assert_eq!(
        state.lock().unwrap().count(|e| matches!(e, Event::Open(_))),
        0
    );
}

#[rstest::rstest]
#[case::commit(false)]
#[case::abort(true)]
#[tokio::test]
async fn commit_drains_writes_and_abort_discards_them(#[case] abort: bool) {
    let state = Arc::new(Mutex::new(State::default()));
    let batch = batch();
    let mut writer = sink(&state)
        .open((), batch.schema(), options())
        .await
        .unwrap();
    writer.write(batch).await.unwrap();
    release(&state);
    if abort {
        writer.abort().await.unwrap();
    } else {
        assert_eq!(writer.commit().await.unwrap().rows_written, 3);
    }
    let state = state.lock().unwrap();
    assert_eq!(state.count(|e| matches!(e, Event::WriteCompleted(_))), 1);
    assert_eq!(
        state.count(|e| matches!(e, Event::Commit { .. })),
        usize::from(!abort)
    );
    assert_eq!(
        state.count(|e| matches!(e, Event::Abort(_))),
        if abort { 2 } else { 0 }
    );
}

#[tokio::test]
async fn async_failure_returns_without_abort_or_publication() {
    let state = Arc::new(Mutex::new(State::failing([Failure::Write])));
    let mut writer = sink(&state)
        .open((), batch().schema(), options())
        .await
        .unwrap();
    writer.write(batch()).await.unwrap();
    writer.write(batch()).await.unwrap();
    release(&state);
    assert!(writer.commit().await.is_err());
    let state = state.lock().unwrap();
    assert_eq!(state.count(|e| matches!(e, Event::Commit { .. })), 0);
    assert_eq!(state.count(|e| matches!(e, Event::Abort(_))), 0);
}

#[tokio::test]
async fn cancelled_write_makes_commit_terminal_without_aborting() {
    let state = Arc::new(Mutex::new(State::default()));
    let mut writer = sink(&state)
        .open((), batch().schema(), options())
        .await
        .unwrap();
    writer.write(batch()).await.unwrap();
    writer.write(batch()).await.unwrap();
    assert!(writer.write(batch()).now_or_never().is_none());
    release(&state);
    assert!(writer.write(batch()).await.is_err());
    assert!(writer.commit().await.is_err());
    let state = state.lock().unwrap();
    assert_eq!(state.count(|e| matches!(e, Event::Commit { .. })), 0);
    assert_eq!(state.count(|e| matches!(e, Event::Abort(_))), 0);
}

#[tokio::test]
async fn schema_error_is_terminal_even_for_empty_batches() {
    let state = Arc::new(Mutex::new(State::default()));
    let mut writer = sink(&state)
        .open((), batch().schema(), options())
        .await
        .unwrap();
    let bad = RecordBatch::new_empty(Arc::new(arrow_schema::Schema::empty()));
    assert!(matches!(
        writer.write(bad).await,
        Err(Error::SchemaMismatch)
    ));
    assert!(writer.commit().await.is_err());
    assert_eq!(
        state
            .lock()
            .unwrap()
            .count(|e| matches!(e, Event::WriteStarted(_))),
        0
    );
    assert_eq!(
        state
            .lock()
            .unwrap()
            .count(|e| matches!(e, Event::Abort(_))),
        0
    );
}

#[tokio::test]
async fn publication_failure_does_not_delete_possibly_published_data() {
    let state = Arc::new(Mutex::new(State::failing([Failure::Commit])));
    let writer = sink(&state)
        .open((), batch().schema(), options())
        .await
        .unwrap();
    assert!(writer.commit().await.is_err());
    let state = state.lock().unwrap();
    assert_eq!(state.count(|e| matches!(e, Event::Commit { .. })), 0);
    assert_eq!(state.count(|e| matches!(e, Event::Abort(_))), 0);
    assert_eq!(state.count(|e| matches!(e, Event::CommitAttempt)), 1);
    assert_eq!(state.count(|e| matches!(e, Event::SessionAbort)), 0);
}

#[tokio::test]
async fn drop_never_commits() {
    let state = Arc::new(Mutex::new(State::default()));
    let mut writer = sink(&state)
        .open((), batch().schema(), options())
        .await
        .unwrap();
    writer.write(batch()).await.unwrap();
    drop(writer);
    release(&state);
    let state = state.lock().unwrap();
    assert_eq!(state.count(|e| matches!(e, Event::WriteCompleted(_))), 0);
    assert_eq!(state.count(|e| matches!(e, Event::Commit { .. })), 0);
}

#[tokio::test]
async fn available_writer_progresses_without_waiting_for_slow_writer() {
    let state = Arc::new(Mutex::new(State::default()));
    let mut writer = sink(&state)
        .open((), batch().schema(), options())
        .await
        .unwrap();
    writer.write(batch()).await.unwrap();
    writer.write(batch()).await.unwrap();
    // Free the writer that actually received the first batch.
    {
        let mut state_guard = state.lock().unwrap();
        let first = state_guard
            .events
            .iter()
            .find_map(|event| match event {
                Event::WriteStarted(id) => Some(*id),
                _ => None,
            })
            .unwrap();
        state_guard.gates.remove(first).send(()).unwrap();
    }
    assert!(writer.write(batch()).now_or_never().unwrap().is_ok());
    assert_eq!(
        state
            .lock()
            .unwrap()
            .count(|e| matches!(e, Event::WriteStarted(_))),
        3
    );
    assert_eq!(
        state
            .lock()
            .unwrap()
            .count(|e| matches!(e, Event::WriteCompleted(_))),
        2
    );
    release(&state);
    assert_eq!(writer.commit().await.unwrap().rows_written, 9);
}

#[tokio::test]
async fn observed_write_failure_is_terminal_and_abort_keeps_all_writers() {
    let state = Arc::new(Mutex::new(State::failing([Failure::Write])));
    let mut writer = sink(&state)
        .open((), batch().schema(), options())
        .await
        .unwrap();
    writer.write(batch()).await.unwrap();
    release(&state);
    assert!(writer.write(batch()).await.is_err());
    assert!(writer.write(batch()).await.is_err());
    writer.abort().await.unwrap();
    assert_eq!(
        state
            .lock()
            .unwrap()
            .count(|e| matches!(e, Event::Abort(_))),
        2
    );
}

#[rstest::rstest]
#[case::finish_failure(false)]
#[case::finish_and_abort_failure(true)]
#[tokio::test]
async fn finish_failure_returns_without_abort_or_publication(#[case] fail_abort: bool) {
    let mut failures = vec![Failure::FirstFinish];
    if fail_abort {
        failures.push(Failure::Abort);
    }
    let state = Arc::new(Mutex::new(State::failing(failures)));
    let writer = sink(&state)
        .open((), batch().schema(), options())
        .await
        .unwrap();
    assert!(writer.commit().await.is_err());
    let state = state.lock().unwrap();
    assert!(state.count(|e| matches!(e, Event::Finish(_))) > 0);
    assert_eq!(state.count(|e| matches!(e, Event::CommitAttempt)), 0);
    assert_eq!(state.count(|e| matches!(e, Event::Abort(_))), 0);
    assert_eq!(state.count(|e| matches!(e, Event::SessionAbort)), 0);
}

#[rstest::rstest]
#[case::wrong_schema(false)]
#[case::wrong_count(true)]
#[tokio::test]
async fn backend_session_contract_is_checked_and_invalid_sessions_are_rejected(
    #[case] wrong_count: bool,
) {
    let state = Arc::new(Mutex::new(State {
        config: Config {
            defect: if wrong_count {
                SessionDefect::WrongCount
            } else {
                SessionDefect::WrongSchema
            },
            ..Default::default()
        },
        ..Default::default()
    }));
    assert!(
        sink(&state)
            .open((), batch().schema(), options())
            .await
            .is_err()
    );
    let state = state.lock().unwrap();
    assert_eq!(state.count(|e| matches!(e, Event::Abort(_))), 0);
    assert_eq!(state.count(|e| matches!(e, Event::SessionAbort)), 0);
    assert_eq!(state.count(|e| matches!(e, Event::CommitAttempt)), 0);
}

#[tokio::test]
async fn abort_attempts_all_writers_and_session_even_when_cleanup_fails() {
    let state = Arc::new(Mutex::new(State::failing([Failure::Abort])));
    let writer = sink(&state)
        .open((), batch().schema(), options())
        .await
        .unwrap();
    assert!(writer.abort().await.is_err());
    let state = state.lock().unwrap();
    assert_eq!(state.count(|e| matches!(e, Event::Abort(_))), 2);
    assert_eq!(state.count(|e| matches!(e, Event::SessionAbort)), 1);
}
