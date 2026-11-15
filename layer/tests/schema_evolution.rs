#[path = "../examples/support/counting_sink.rs"]
mod counting_sink;

use arrow_array::{Int32Array, RecordBatch};
use arrow_schema::{DataType, Field};
use counting_sink::CountingSink;
use open_arrow_sink_core::{Error, ParallelSink, Sink, SinkExt, WriteOptions};
use open_arrow_sink_layer::{FieldRemap, SchemaEvolutionLayer, UnorderedWriteLayer};
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

fn input() -> RecordBatch {
    RecordBatch::try_from_iter([("old", Arc::new(Int32Array::from(vec![1, 2, 3])) as _)]).unwrap()
}
fn layer() -> SchemaEvolutionLayer {
    SchemaEvolutionLayer::new(vec![FieldRemap::new(
        "old",
        Field::new("new", DataType::Int32, false),
        Ok,
    )])
}
fn options() -> WriteOptions {
    WriteOptions {
        ordering: open_arrow_sink_core::Ordering::Unordered,
        ..Default::default()
    }
}

#[tokio::test]
async fn single_and_parallel_sessions_keep_input_schema_and_commit() {
    for parallel in [false, true] {
        let sink = CountingSink.layer(layer());
        let sink: Box<dyn Sink<Request = Arc<AtomicU64>>> = if parallel {
            Box::new(sink.layer(UnorderedWriteLayer::new(
                std::num::NonZeroUsize::new(2).unwrap(),
            )))
        } else {
            Box::new(sink)
        };
        let input = input();
        let committed = Arc::new(AtomicU64::new(0));
        let mut writer = sink
            .open(committed.clone(), input.schema(), options())
            .await
            .unwrap();
        assert_eq!(writer.schema(), &input.schema());
        assert!(!sink.capability().supports_blob_streaming);
        writer.write(input.clone()).await.unwrap();
        writer.write(input.slice(0, 0)).await.unwrap();
        assert_eq!(writer.commit().await.unwrap().rows_written, 3);
        assert_eq!(committed.load(Ordering::SeqCst), 3);
    }
}

#[tokio::test]
async fn failed_transform_is_terminal_and_cannot_publish() {
    let sink = CountingSink.layer(SchemaEvolutionLayer::new(vec![FieldRemap::new(
        "old",
        Field::new("new", DataType::Int32, false),
        |_| Err(Error::Unsupported("conversion failed".into())),
    )]));
    let input = input();
    let committed = Arc::new(AtomicU64::new(0));
    let mut writer = sink
        .open(committed.clone(), input.schema(), options())
        .await
        .unwrap();
    assert!(writer.write(input.clone()).await.is_err());
    assert!(writer.write(input.clone()).await.is_err());
    assert!(writer.commit().await.is_err());
    assert_eq!(committed.load(Ordering::SeqCst), 0);

    let mut session = sink
        .open_writers(
            committed.clone(),
            input.schema(),
            options(),
            std::num::NonZeroUsize::MIN,
        )
        .await
        .unwrap();
    let mut writer = session.writers.pop().unwrap();
    assert!(writer.write(input).await.is_err());
    assert!(writer.finish().await.is_err());
    writer.abort().await.unwrap();
    session.committer.abort().await.unwrap();
    assert_eq!(committed.load(Ordering::SeqCst), 0);
}
