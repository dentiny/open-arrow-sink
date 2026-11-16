use counting_sink::CountingSink;

use arrow_array::{Int32Array, RecordBatch};
use arrow_schema::{DataType, Field};
use open_arrow_sink_core::{ParallelSink, Sink, SinkExt, WriteOptions};
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
    WriteOptions::default().with_ordering(open_arrow_sink_core::Ordering::Unordered)
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

// Test-local sink: publish the accepted row count only on commit.
mod counting_sink {
    use super::*;
    use open_arrow_sink_core::{
        CompressionCapability, DataWriter, Result, SchemaRef, SessionCommitter, SinkCapability,
        WriteMode, WriteSession, WriteSummary, Writer, async_trait, validation,
    };
    use std::num::NonZeroUsize;

    pub(super) struct CountingSink;
    #[async_trait]
    impl Sink for CountingSink {
        type Request = Arc<AtomicU64>;
        fn capability(&self) -> SinkCapability {
            SinkCapability::default()
                .with_supported_modes(&[WriteMode::Create])
                .with_ordering(open_arrow_sink_core::Ordering::Unordered)
                .with_compression(CompressionCapability::default())
                .with_supported_partitions(vec![])
                .with_supports_blob_handle(false)
                .with_supports_blob_streaming(false)
                .with_supports_storage_version(false)
                .with_supported_indexes(&[])
        }
        async fn open(
            &self,
            request: Self::Request,
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
    impl ParallelSink for CountingSink {
        type Output = u64;
        async fn open_writers(
            &self,
            request: Self::Request,
            schema: SchemaRef,
            options: WriteOptions,
            count: NonZeroUsize,
        ) -> Result<WriteSession<u64>> {
            validation::validate_options(&schema, &options, self.capability())?;
            assert_eq!(schema.field(0).name(), "new");
            Ok(WriteSession {
                writers: (0..count.get())
                    .map(|_| {
                        Box::new(CountWriter {
                            schema: schema.clone(),
                            rows: 0,
                        }) as Box<dyn DataWriter<Output = u64>>
                    })
                    .collect(),
                committer: Box::new(CountCommitter(request)),
            })
        }
    }
    struct CountWriter {
        schema: SchemaRef,
        rows: u64,
    }
    #[async_trait]
    impl DataWriter for CountWriter {
        type Output = u64;
        fn schema(&self) -> &SchemaRef {
            &self.schema
        }
        async fn write(&mut self, batch: RecordBatch) -> Result<()> {
            validation::validate_batch(&self.schema, &batch)?;
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
    struct CountCommitter(Arc<AtomicU64>);
    #[async_trait]
    impl SessionCommitter<u64> for CountCommitter {
        async fn commit(self: Box<Self>, outputs: Vec<u64>) -> Result<WriteSummary> {
            let rows_written = outputs.into_iter().sum();
            self.0.store(rows_written, Ordering::SeqCst);
            Ok(WriteSummary { rows_written })
        }
        async fn abort(self: Box<Self>) -> Result<()> {
            Ok(())
        }
    }
}
