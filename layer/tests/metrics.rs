#[path = "support/parallel_sink.rs"]
mod parallel_sink;
use arrow_array::{ArrayRef, Int64Array};
use futures::FutureExt;
use open_arrow_sink_core::{Error, ParallelSink, RecordBatch, Sink, SinkExt};
use open_arrow_sink_layer::{MetricsLayer, UnorderedWriteLayer};
use parallel_sink::{Config, Failure, State, TestSink};
use std::{num::NonZeroUsize, sync::Arc};

#[derive(Default, Clone, Copy)]
struct Behavior {
    blocked: bool,
    unmeasured: bool,
    fail_commit: bool,
}
fn sink(behavior: Behavior) -> TestSink {
    TestSink(Arc::new(std::sync::Mutex::new(State {
        config: Config {
            ordering: open_arrow_sink_core::Ordering::Unordered,
            gated: behavior.blocked,
            record_io: !behavior.unmeasured,
            write_bytes: 10,
            commit_read_bytes: 5,
            failures: if behavior.fail_commit {
                [Failure::Commit].into()
            } else {
                Default::default()
            },
            ..Default::default()
        },
        ..Default::default()
    })))
}
fn input() -> RecordBatch {
    RecordBatch::try_from_iter([("id", Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef)])
        .unwrap()
}
use open_arrow_sink_test_support::unordered_options as options;

#[rstest::rstest]
#[case::single(0)]
#[case::inside_parallel(1)]
#[case::outside_parallel(2)]
#[tokio::test]
async fn measures_one_commit_per_session_without_double_counting(#[case] placement: u8) {
    let layer = MetricsLayer::default();
    let metrics = layer.metrics();
    let unordered = UnorderedWriteLayer::new(NonZeroUsize::new(3).unwrap());
    let sink: Box<dyn Sink<Request = ()>> = match placement {
        0 => Box::new(sink(Behavior::default()).layer(layer)),
        1 => Box::new(sink(Behavior::default()).layer(layer).layer(unordered)),
        _ => Box::new(sink(Behavior::default()).layer(unordered).layer(layer)),
    };
    for _ in 0..2 {
        let mut writer = sink.open((), input().schema(), options()).await.unwrap();
        for _ in 0..4 {
            writer.write(input()).await.unwrap();
        }
        assert_eq!(writer.commit().await.unwrap().rows_written, 12);
    }
    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.sessions_opened, 2);
    assert_eq!(snapshot.sessions_committed, 2);
    assert_eq!(snapshot.batches_accepted, 8);
    assert_eq!(snapshot.rows_accepted, 24);
    assert_eq!(snapshot.rows_committed, 24);
    assert_eq!(snapshot.bytes_read, 10);
    assert_eq!(snapshot.bytes_written, 80);
    assert_eq!(snapshot.failed_operations, 0);
}

#[tokio::test]
async fn cancellation_is_terminal_without_automatic_abort() {
    let layer = MetricsLayer::default();
    let metrics = layer.metrics();
    let sink = sink(Behavior {
        blocked: true,
        ..Default::default()
    })
    .layer(layer);
    let mut writer = sink.open((), input().schema(), options()).await.unwrap();
    assert!(writer.write(input()).now_or_never().is_none());
    assert_eq!(metrics.snapshot().failed_operations, 1);
    assert!(matches!(writer.commit().await, Err(Error::Poisoned)));
    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.sessions_committed, 0);
    assert_eq!(snapshot.sessions_aborted, 0);
    assert_eq!(snapshot.rows_accepted, 0);
    assert_eq!(snapshot.failed_operations, 1);
    assert_eq!(snapshot.bytes_written, 10);
}

#[tokio::test]
async fn uninstrumented_sessions_leave_measured_totals_intact() {
    let layer = MetricsLayer::default();
    let metrics = layer.metrics();
    for unmeasured in [false, true, false] {
        sink(Behavior {
            unmeasured,
            ..Default::default()
        })
        .layer(layer.clone())
        .open((), input().schema(), options())
        .await
        .unwrap()
        .commit()
        .await
        .unwrap();
    }
    assert_eq!(metrics.snapshot().sessions_committed, 3);
    assert_eq!(metrics.snapshot().bytes_read, 10);
    assert_eq!(metrics.snapshot().bytes_written, 0);
    sink(Behavior::default())
        .layer(layer)
        .open((), input().schema(), options())
        .await
        .unwrap()
        .abort()
        .await
        .unwrap();
    assert_eq!(metrics.snapshot().sessions_aborted, 1);
}

#[rstest::rstest]
#[case::abort(false)]
#[case::failed_commit(true)]
#[tokio::test]
async fn io_is_visible_before_commit_and_retained_after_failure(#[case] fail_commit: bool) {
    let layer = MetricsLayer::default();
    let metrics = layer.metrics();
    let sink = sink(Behavior {
        fail_commit,
        ..Default::default()
    })
    .layer(layer);
    let mut writer = sink.open((), input().schema(), options()).await.unwrap();
    writer.write(input()).await.unwrap();
    assert_eq!(metrics.snapshot().bytes_written, 10);
    assert_eq!(metrics.snapshot().rows_committed, 0);
    if fail_commit {
        assert!(writer.commit().await.is_err());
        assert_eq!(metrics.snapshot().bytes_read, 5);
    } else {
        writer.abort().await.unwrap();
    }
    assert_eq!(metrics.snapshot().bytes_written, 10);
    assert_eq!(metrics.snapshot().sessions_committed, 0);
}

#[tokio::test]
async fn shared_handle_in_nested_layers_does_not_duplicate_io() {
    let metrics = open_arrow_sink_layer::WriteMetrics::default();
    let sink = sink(Behavior::default())
        .layer(MetricsLayer::new(metrics.clone()))
        .layer(MetricsLayer::new(metrics.clone()));
    let mut writer = sink.open((), input().schema(), options()).await.unwrap();
    writer.write(input()).await.unwrap();
    writer.commit().await.unwrap();
    assert_eq!(metrics.snapshot().bytes_written, 10);
    assert_eq!(metrics.snapshot().bytes_read, 5);
}

#[rstest::rstest]
#[case::write_error(false, false)]
#[case::blob_open_error(true, false)]
#[case::abort_after_write_error(false, true)]
#[tokio::test]
async fn poisoned_writer_operations_do_not_change_metrics(
    #[case] blob_error: bool,
    #[case] abort: bool,
) {
    let layer = MetricsLayer::default();
    let metrics = layer.metrics();
    let backend = sink(Behavior::default());
    backend
        .0
        .lock()
        .unwrap()
        .config
        .failures
        .insert(Failure::Write);
    let sink = backend.layer(layer);
    let mut writer = sink.open((), input().schema(), options()).await.unwrap();
    if blob_error {
        assert!(matches!(
            writer.create_blob().await,
            Err(Error::Unsupported(_))
        ));
    } else {
        assert!(matches!(
            writer.write(input()).await,
            Err(Error::Unsupported(_))
        ));
    }
    let before = metrics.snapshot();
    assert_eq!(before.failed_operations, 1);
    assert!(matches!(writer.write(input()).await, Err(Error::Poisoned)));
    assert!(matches!(
        writer.write_with_blobs(input(), vec![]).await,
        Err(Error::Poisoned)
    ));
    assert!(matches!(writer.create_blob().await, Err(Error::Poisoned)));
    assert_eq!(metrics.snapshot(), before);
    if abort {
        writer.abort().await.unwrap();
        assert_eq!(metrics.snapshot().sessions_aborted, 1);
        assert_eq!(metrics.snapshot().failed_operations, 1);
    } else {
        assert!(matches!(writer.commit().await, Err(Error::Poisoned)));
        assert_eq!(metrics.snapshot(), before);
    }
}

#[rstest::rstest]
#[case::cancelled_write(true, false)]
#[case::failed_write(false, false)]
#[case::finished_writer(false, true)]
#[tokio::test]
async fn poisoned_data_writer_operations_do_not_change_metrics(
    #[case] cancelled: bool,
    #[case] finished: bool,
) {
    let layer = MetricsLayer::default();
    let metrics = layer.metrics();
    let backend = sink(Behavior {
        blocked: cancelled,
        ..Default::default()
    });
    if !cancelled && !finished {
        backend
            .0
            .lock()
            .unwrap()
            .config
            .failures
            .insert(Failure::Write);
    }
    let sink = backend.layer(layer);
    let mut session = sink
        .open_writers((), input().schema(), options(), NonZeroUsize::MIN)
        .await
        .unwrap();
    let writer = &mut session.writers[0];
    if finished {
        writer.write(input()).await.unwrap();
        assert_eq!(writer.finish().await.unwrap(), 3);
    } else if cancelled {
        assert!(writer.write(input()).now_or_never().is_none());
    } else {
        assert!(matches!(
            writer.write(input()).await,
            Err(Error::Unsupported(_))
        ));
    }
    let before = metrics.snapshot();
    assert_eq!(before.failed_operations, if finished { 0 } else { 1 });
    assert!(matches!(writer.write(input()).await, Err(Error::Poisoned)));
    assert!(matches!(writer.finish().await, Err(Error::Poisoned)));
    assert_eq!(metrics.snapshot(), before);
    session.writers.pop().unwrap().abort().await.unwrap();
    session.committer.abort().await.unwrap();
    assert_eq!(metrics.snapshot().sessions_aborted, 1);
    assert_eq!(
        metrics.snapshot().failed_operations,
        before.failed_operations
    );
}
