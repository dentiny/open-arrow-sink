use std::sync::{Arc, Mutex};

use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::Schema;
use futures::executor::block_on;
use open_arrow_sink_core::{
    Error, Ordering, Result, SchemaRef, Sink, SinkCapability, SinkExt, WriteOptions, WriteSummary,
    Writer, async_trait, validation,
};
use open_arrow_sink_layer::OrderingLayer;

struct TestSink {
    ordering: Ordering,
    opened: Arc<Mutex<Vec<WriteOptions>>>,
}

struct TestWriter(SchemaRef, u64);

#[async_trait]
impl Writer for TestWriter {
    fn schema(&self) -> &SchemaRef {
        &self.0
    }

    async fn write(&mut self, batch: RecordBatch) -> Result<()> {
        validation::validate_batch(&self.0, &batch)?;
        self.1 += batch.num_rows() as u64;
        Ok(())
    }

    async fn commit(self: Box<Self>) -> Result<WriteSummary> {
        Ok(WriteSummary {
            rows_written: self.1,
        })
    }

    async fn abort(self: Box<Self>) -> Result<()> {
        Ok(())
    }
}

#[async_trait]
impl Sink for TestSink {
    type Request = ();

    fn capability(&self) -> SinkCapability {
        SinkCapability {
            ordering: self.ordering,
        }
    }

    async fn open(
        &self,
        _: (),
        schema: SchemaRef,
        options: WriteOptions,
    ) -> Result<Box<dyn Writer>> {
        validation::validate_options(&options, self.capability())?;
        self.opened.lock().unwrap().push(options);
        Ok(Box::new(TestWriter(schema, 0)))
    }
}

#[test]
fn unordered_policy_reaches_backend_and_writer_is_usable() {
    block_on(async {
        let opened = Arc::new(Mutex::new(Vec::new()));
        let sink = TestSink {
            ordering: Ordering::Unordered,
            opened: opened.clone(),
        }
        .layer(OrderingLayer::new(Ordering::Unordered));
        assert_eq!(sink.capability().ordering, Ordering::Unordered);
        let batch = RecordBatch::try_from_iter([(
            "value",
            Arc::new(Int64Array::from(vec![1, 2, 3])) as arrow_array::ArrayRef,
        )])
        .unwrap();
        let schema = batch.schema();
        let mut writer = sink
            .open((), schema.clone(), WriteOptions::default())
            .await
            .unwrap();
        writer.write(batch).await.unwrap();
        assert_eq!(writer.commit().await.unwrap().rows_written, 3);
        assert_eq!(opened.lock().unwrap()[0].ordering, Ordering::Unordered);
    });
}

#[test]
fn unsupported_ordering_is_rejected_before_backend_open() {
    block_on(async {
        let opened = Arc::new(Mutex::new(Vec::new()));
        let sink = TestSink {
            ordering: Ordering::Unordered,
            opened: opened.clone(),
        }
        .layer(OrderingLayer::new(Ordering::PreserveInput));
        let result = sink
            .open(
                (),
                Arc::new(Schema::empty()),
                WriteOptions {
                    ordering: Ordering::Unordered,
                },
            )
            .await;
        assert!(matches!(result, Err(Error::Unsupported(_))));
        assert!(opened.lock().unwrap().is_empty());
    });
}
