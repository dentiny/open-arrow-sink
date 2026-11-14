use std::{path::PathBuf, sync::Arc};

use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use lance::{Dataset, index::DatasetIndexExt};
use open_arrow_sink_core::{IndexSpec, IndexType, Ordering, Sink, WriteOptions};
use open_arrow_sink_lance::LanceDatasetSink;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let destination = std::env::args_os()
        .nth(1)
        .map(PathBuf::from)
        .ok_or("usage: write_indexed <new-dataset-directory>")?;
    let batch = RecordBatch::try_from_iter([
        ("id", Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef),
        (
            "text",
            Arc::new(StringArray::from(vec![
                "hello world",
                "hello lance",
                "goodbye world",
            ])) as ArrayRef,
        ),
    ])?;
    let mut writer = LanceDatasetSink
        .open(
            destination.clone(),
            batch.schema(),
            WriteOptions {
                ordering: Ordering::Unordered,
                indexes: vec![
                    IndexSpec::new("id", IndexType::Scalar),
                    IndexSpec::new("text", IndexType::Text),
                ],
                ..Default::default()
            },
        )
        .await?;
    writer.write(batch).await?;
    let summary = writer.commit().await?;
    let dataset = Dataset::open(destination.to_str().ok_or("destination must be UTF-8")?).await?;
    println!(
        "Committed {} rows and built {} indexes in {}",
        summary.rows_written,
        dataset.load_indices().await?.len(),
        destination.display()
    );
    Ok(())
}
