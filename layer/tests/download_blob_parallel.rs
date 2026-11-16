#[path = "support/parallel_sink.rs"]
mod parallel_sink;
use open_arrow_sink_core::{RecordBatch, Sink, SinkExt};
use open_arrow_sink_layer::UnorderedWriteLayer;
use open_arrow_sink_test_support::unordered_options as options;
use parallel_sink::{Config, Event, State, TestSink};
use std::{
    num::NonZeroUsize,
    sync::{Arc, Mutex},
};
fn sink(state: &Arc<Mutex<State>>) -> impl Sink<Request = ()> {
    TestSink(state.clone()).layer(UnorderedWriteLayer::new(NonZeroUsize::new(2).unwrap()))
}

#[tokio::test]
async fn unordered_layer_preserves_blob_configuration_but_rejects_streaming() {
    use arrow_array::StringArray;
    use open_arrow_sink_layer::DownloadBlobLayer;

    let state = Arc::new(Mutex::new(State {
        config: Config {
            supports_blobs: true,
            ..Default::default()
        },
        ..Default::default()
    }));
    assert!(TestSink(state.clone()).capability().supports_blob_handle);
    let sink = sink(&state);
    assert!(sink.capability().supports_blob_handle);
    assert!(!sink.capability().supports_blob_streaming);
    let input = RecordBatch::try_from_iter([(
        "url",
        Arc::new(StringArray::from(vec!["https://example.invalid/blob"])) as _,
    )])
    .unwrap();
    // Native logical blob options remain valid for parallel data writers.
    sink.open(
        (),
        input.schema(),
        options().with_blob_handle(Default::default()),
    )
    .await
    .unwrap()
    .abort()
    .await
    .unwrap();
    let opened = state.lock().unwrap().count(|e| matches!(e, Event::Open(_)));
    assert_eq!(opened, 2);
    assert!(
        sink.layer(DownloadBlobLayer::new(["url".to_owned()].into()))
            .open((), input.schema(), options())
            .await
            .is_err()
    );
    assert_eq!(
        state.lock().unwrap().count(|e| matches!(e, Event::Open(_))),
        opened
    );
}
