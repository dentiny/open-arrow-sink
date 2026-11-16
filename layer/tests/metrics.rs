use arrow_array::{ArrayRef, Int64Array};
use futures::FutureExt;
use open_arrow_sink_core::{
    DataWriter, Ordering, ParallelSink, RecordBatch, Result, SchemaRef, SessionCommitter, Sink,
    SinkCapability, SinkExt, WriteMode, WriteOptions, WriteSession, WriteSummary, Writer,
    async_trait,
};
use open_arrow_sink_layer::{MetricsLayer, UnorderedWriteLayer};
use std::{num::NonZeroUsize, sync::Arc};

#[derive(Default, Clone, Copy)]
struct Behavior {
    blocked: bool,
    unmeasured: bool,
    fail_commit: bool,
}
struct TestSink;
struct Data {
    schema: SchemaRef,
    rows: u64,
    blocked: bool,
    io: Option<open_arrow_sink_core::IoMetrics>,
}
struct Committer {
    io: Option<open_arrow_sink_core::IoMetrics>,
    fail_commit: bool,
}
#[async_trait]
impl DataWriter for Data {
    type Output = u64;
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }
    async fn write(&mut self, batch: RecordBatch) -> Result<()> {
        if let Some(io) = &self.io {
            io.record_write(10);
        }
        if self.blocked {
            futures::future::pending::<()>().await;
        }
        self.rows += batch.num_rows() as u64;
        Ok(())
    }
    async fn finish(&mut self) -> Result<u64> {
        Ok(self.rows)
    }
    async fn abort(self: Box<Self>) -> Result<()> {
        Ok(())
    }
}
#[async_trait]
impl SessionCommitter<u64> for Committer {
    async fn commit(self: Box<Self>, outputs: Vec<u64>) -> Result<WriteSummary> {
        if let Some(io) = &self.io {
            io.record_read(5);
        }
        if self.fail_commit {
            return Err(open_arrow_sink_core::Error::Unsupported(
                "commit failed".into(),
            ));
        }
        Ok(WriteSummary {
            rows_written: outputs.iter().sum(),
        })
    }
    async fn abort(self: Box<Self>) -> Result<()> {
        Ok(())
    }
}
#[async_trait]
impl Sink for TestSink {
    type Request = Behavior;
    fn capability(&self) -> SinkCapability {
        SinkCapability {
            supported_modes: &[WriteMode::Create],
            ..Default::default()
        }
    }
    async fn open(
        &self,
        request: Behavior,
        schema: SchemaRef,
        options: WriteOptions,
    ) -> Result<Box<dyn Writer>> {
        self.open_writers(request, schema, options, NonZeroUsize::MIN)
            .await?
            .into_writer()
            .await
    }
}
#[async_trait]
impl ParallelSink for TestSink {
    type Output = u64;
    async fn open_writers(
        &self,
        request: Behavior,
        schema: SchemaRef,
        options: WriteOptions,
        count: NonZeroUsize,
    ) -> Result<WriteSession<u64>> {
        let io = options.io_metrics.filter(|_| !request.unmeasured);
        Ok(WriteSession {
            writers: (0..count.get())
                .map(|_| {
                    Box::new(Data {
                        schema: schema.clone(),
                        rows: 0,
                        blocked: request.blocked,
                        io: io.clone(),
                    }) as Box<dyn DataWriter<Output = u64>>
                })
                .collect(),
            committer: Box::new(Committer {
                io,
                fail_commit: request.fail_commit,
            }),
        })
    }
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
    let sink: Box<dyn Sink<Request = Behavior>> = match placement {
        0 => Box::new(TestSink.layer(layer)),
        1 => Box::new(TestSink.layer(layer).layer(unordered)),
        _ => Box::new(TestSink.layer(unordered).layer(layer)),
    };
    for _ in 0..2 {
        let mut writer = sink
            .open(Behavior::default(), input().schema(), options())
            .await
            .unwrap();
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
async fn cancellation_and_abort_are_observable_and_terminal() {
    let layer = MetricsLayer::default();
    let metrics = layer.metrics();
    let sink = TestSink.layer(layer);
    let mut writer = sink
        .open(
            Behavior {
                blocked: true,
                ..Default::default()
            },
            input().schema(),
            options(),
        )
        .await
        .unwrap();
    assert!(writer.write(input()).now_or_never().is_none());
    assert_eq!(metrics.snapshot().failed_operations, 1);
    assert!(writer.commit().await.is_err());
    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.sessions_committed, 0);
    assert_eq!(snapshot.sessions_aborted, 1);
    assert_eq!(snapshot.rows_accepted, 0);
    assert_eq!(snapshot.failed_operations, 2);
    assert_eq!(snapshot.bytes_written, 10);
}

#[tokio::test]
async fn uninstrumented_sessions_leave_measured_totals_intact() {
    let layer = MetricsLayer::default();
    let metrics = layer.metrics();
    let sink = TestSink.layer(layer);
    for unmeasured in [false, true, false] {
        sink.open(
            Behavior {
                unmeasured,
                ..Default::default()
            },
            input().schema(),
            options(),
        )
        .await
        .unwrap()
        .commit()
        .await
        .unwrap();
    }
    assert_eq!(metrics.snapshot().sessions_committed, 3);
    assert_eq!(metrics.snapshot().bytes_read, 10);
    assert_eq!(metrics.snapshot().bytes_written, 0);
    sink.open(Behavior::default(), input().schema(), options())
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
    let sink = TestSink.layer(layer);
    let mut writer = sink
        .open(
            Behavior {
                fail_commit,
                ..Default::default()
            },
            input().schema(),
            options(),
        )
        .await
        .unwrap();
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
    let sink = TestSink
        .layer(MetricsLayer::new(metrics.clone()))
        .layer(MetricsLayer::new(metrics.clone()));
    let mut writer = sink
        .open(Behavior::default(), input().schema(), options())
        .await
        .unwrap();
    writer.write(input()).await.unwrap();
    writer.commit().await.unwrap();
    assert_eq!(metrics.snapshot().bytes_written, 10);
    assert_eq!(metrics.snapshot().bytes_read, 5);
}
