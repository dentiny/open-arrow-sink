use std::{num::NonZeroUsize, path::PathBuf, sync::Arc};

use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use open_arrow_sink_core::{Ordering, Sink, SinkExt, WriteOptions};
use open_arrow_sink_lance::{
    LanceDatasetSink, LanceIndexKind, LanceIndexLayer, LanceIndexSpec, LancePartitionLayer,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let destination = std::env::args_os()
        .nth(1)
        .map(PathBuf::from)
        .ok_or("usage: write_partitioned <new-root-directory>")?;
    let batch = RecordBatch::try_from_iter([
        (
            "region",
            Arc::new(StringArray::from(vec!["east", "west", "east"])) as ArrayRef,
        ),
        ("id", Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef),
    ])?;
    let sink = LanceDatasetSink
        .layer(LanceIndexLayer::new(vec![LanceIndexSpec::new(
            "id",
            LanceIndexKind::Scalar,
        )]))
        .layer(LancePartitionLayer::new(
            vec!["region".into()],
            NonZeroUsize::new(16).unwrap(),
        ));
    let mut writer = sink
        .open(
            destination.clone(),
            batch.schema(),
            WriteOptions {
                ordering: Ordering::Unordered,
                ..Default::default()
            },
        )
        .await?;
    writer.write(batch).await?;
    let summary = writer.commit().await?;
    println!(
        "Committed {} rows in partition datasets under {}",
        summary.rows_written,
        destination.display()
    );
    Ok(())
}
