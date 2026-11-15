use std::sync::{Arc, Mutex};

use arrow_schema::{DataType, Field, Schema};
use open_arrow_sink_core::{
    Compression, CompressionCapability, CompressionCodec as Codec, CompressionOptions, Error,
    Ordering, Result, SchemaRef, Sink, SinkCapability, SinkExt, WriteOptions, Writer, async_trait,
};
use open_arrow_sink_layer::CompressionLayer;

type Captured = Arc<Mutex<Option<(String, SchemaRef, WriteOptions)>>>;

struct ProbeSink {
    captured: Captured,
}

#[async_trait]
impl Sink for ProbeSink {
    type Request = String;

    fn capability(&self) -> SinkCapability {
        SinkCapability {
            ordering: Ordering::PreserveInput,
            compression: CompressionCapability {
                global: &[Codec::Snappy, Codec::Zstd],
                per_column: &[Codec::Snappy, Codec::Zstd],
            },
            supported_partitions: vec![],
            supported_indexes: &[],
        }
    }

    async fn open(
        &self,
        request: String,
        schema: SchemaRef,
        options: WriteOptions,
    ) -> Result<Box<dyn Writer>> {
        *self.captured.lock().unwrap() = Some((request, schema, options));
        Err(Error::Unsupported("probe reached".into()))
    }
}

#[test]
fn layer_replaces_policy_and_preserves_other_session_inputs() {
    let captured = Captured::default();
    let backend = ProbeSink {
        captured: captured.clone(),
    };
    let capability = backend.capability();
    let compression = CompressionOptions {
        global: Some(Compression::Zstd { level: Some(9) }),
        columns: [("id".into(), Compression::Zstd { level: Some(3) })].into(),
    };
    let sink = backend.layer(CompressionLayer::new(compression.clone()));
    assert_eq!(sink.capability(), capability);
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let options = WriteOptions {
        compression: CompressionOptions {
            global: Some(Compression::Zstd { level: Some(1) }),
            ..Default::default()
        },
        target_file_size: Some(256),
        cleanup_on_error: false,
        ..Default::default()
    };
    let result = futures::executor::block_on(sink.open(
        "destination".into(),
        schema.clone(),
        options.clone(),
    ));
    assert!(matches!(result, Err(Error::Unsupported(message)) if message == "probe reached"));
    let actual = captured.lock().unwrap().take().unwrap();
    assert_eq!(actual.0, "destination");
    assert_eq!(actual.1, schema);
    assert_eq!(
        actual.2,
        WriteOptions {
            compression,
            ..options
        }
    );
}

#[test]
fn invalid_policy_never_reaches_inner_open() {
    for compression in [
        CompressionOptions {
            global: Some(Compression::Gzip),
            ..Default::default()
        },
        CompressionOptions {
            columns: [("missing".into(), Compression::Snappy)].into(),
            ..Default::default()
        },
    ] {
        let captured = Captured::default();
        let sink = ProbeSink {
            captured: captured.clone(),
        }
        .layer(CompressionLayer::new(compression));
        let result = futures::executor::block_on(sink.open(
            "destination".into(),
            Arc::new(Schema::empty()),
            WriteOptions::default(),
        ));
        assert!(matches!(result, Err(Error::Unsupported(_))));
        assert!(captured.lock().unwrap().is_none());
    }
}
