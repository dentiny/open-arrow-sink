//! Run: cargo run -p open-arrow-sink-layer --example custom_sink
#[path = "support/counting_sink.rs"]
mod counting_sink;

use arrow_array::{ArrayRef, Int64Array, RecordBatch};
use counting_sink::CountingSink;
use open_arrow_sink_core::{Ordering, Sink, SinkExt, WriteOptions};
use open_arrow_sink_layer::UnorderedWriteLayer;
use std::{
    num::NonZeroUsize,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering as AtomicOrdering},
    },
};

fn main() -> open_arrow_sink_core::Result<()> {
    futures::executor::block_on(async {
        let published = Arc::new(AtomicU64::new(0));
        let batch = RecordBatch::try_from_iter([(
            "id",
            Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef,
        )])
        .map_err(|error| open_arrow_sink_core::Error::Backend(Box::new(error)))?;
        let sink = CountingSink.layer(UnorderedWriteLayer::new(NonZeroUsize::new(2).unwrap()));
        let mut writer = sink
            .open(
                published.clone(),
                batch.schema(),
                WriteOptions {
                    ordering: Ordering::Unordered,
                    ..WriteOptions::default()
                },
            )
            .await?;
        writer.write(batch.clone()).await?;
        writer.write(batch).await?;
        assert_eq!(published.load(AtomicOrdering::SeqCst), 0);
        let summary = writer.commit().await?;
        assert_eq!(summary.rows_written, 6);
        assert_eq!(published.load(AtomicOrdering::SeqCst), 6);
        println!("Published {} rows in one session", summary.rows_written);
        Ok(())
    })
}
