use arrow_array::{Int32Array, RecordBatch};
use arrow_schema::{DataType, Field};
use open_arrow_sink_core::{Sink, SinkExt};
use open_arrow_sink_layer::{FieldRemap, SchemaEvolutionLayer, UnorderedWriteLayer};
use open_arrow_sink_test_support::parallel_sink::{Config, State, TestSink};
use open_arrow_sink_test_support::unordered_options as options;
use std::sync::Arc;

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

#[rstest::rstest]
#[case::single(false)]
#[case::parallel(true)]
#[tokio::test]
async fn single_and_parallel_sessions_keep_input_schema_and_commit(#[case] parallel: bool) {
    let state = State::shared(Config {
        ordering: open_arrow_sink_core::Ordering::Unordered,
        gated: false,
        ..Default::default()
    });
    let sink = TestSink(state.clone()).layer(layer());
    let sink: Box<dyn Sink<Request = ()>> = if parallel {
        Box::new(sink.layer(UnorderedWriteLayer::new(
            std::num::NonZeroUsize::new(2).unwrap(),
        )))
    } else {
        Box::new(sink)
    };
    let input = input();
    let mut writer = sink.open((), input.schema(), options()).await.unwrap();
    assert_eq!(writer.schema(), &input.schema());
    assert!(!sink.capability().supports_blob_streaming);
    writer.write(input.clone()).await.unwrap();
    writer.write(input.slice(0, 0)).await.unwrap();
    assert_eq!(writer.commit().await.unwrap().rows_written, 3);
    let state = state.lock().unwrap();
    assert_eq!(state.committed_rows(), Some(3));
    assert_eq!(state.schemas.len(), 1);
    assert_eq!(state.schemas[0].field(0).name(), "new");
}
