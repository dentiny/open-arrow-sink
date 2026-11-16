use std::sync::Arc;

use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use lance::index::DatasetIndexExt;
use open_arrow_sink_core::{IndexSpec, IndexType, Ordering, Sink, WriteOptions};
use open_arrow_sink_lance::{LanceDatasetSink, LanceOptions, Url};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let argument = std::env::args()
        .nth(1)
        .ok_or("pass a new dataset URL or path")?;
    let destination = match Url::parse(&argument) {
        Ok(url) => url,
        Err(_) => {
            Url::from_directory_path(std::path::absolute(argument)?).map_err(|_| "invalid path")?
        }
    };
    let sink = LanceDatasetSink::default().with_options(LanceOptions {
        indexes: vec![
            IndexSpec::new("id", IndexType::Scalar),
            IndexSpec::new("text", IndexType::Text),
        ],
        ..Default::default()
    });
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
    let dataset = sink.dataset_builder(destination.clone())?.load().await?;
    println!(
        "Committed {} rows and built {} indexes in {}",
        summary.rows_written,
        dataset.load_indices().await?.len(),
        destination
    );
    Ok(())
}
