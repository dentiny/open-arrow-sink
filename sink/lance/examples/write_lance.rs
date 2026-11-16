use std::sync::Arc;

use arrow_array::{ArrayRef, Int64Array, RecordBatch};
use open_arrow_sink_core::{Ordering, Sink, WriteOptions};
use open_arrow_sink_lance::{LanceDatasetSink, Url};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let argument = std::env::args()
        .nth(1)
        .ok_or("pass a new dataset URL or path")?;
    let path = match Url::parse(&argument) {
        Ok(url) => url,
        Err(_) => {
            Url::from_directory_path(std::path::absolute(argument)?).map_err(|_| "invalid path")?
        }
    };
    let sink = LanceDatasetSink::default();
    let batch = RecordBatch::try_from_iter([(
        "value",
        Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef,
    )])?;
    let mut writer = sink
        .open(
            path.clone(),
            batch.schema(),
            WriteOptions::default().with_ordering(Ordering::Unordered),
        )
        .await?;
    writer.write(batch.clone()).await?;
    writer.write(batch).await?;
    writer.commit().await?;
    let dataset = sink.dataset_builder(path.clone())?.load().await?;
    let output = dataset.scan().try_into_batch().await?;
    let rows = output.num_rows();
    let mut values = output
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or("unexpected column type")?
        .values()
        .to_vec();
    values.sort_unstable();
    assert_eq!(values, vec![1, 1, 2, 2, 3, 3]);

    assert_eq!(rows, 6);
    println!("committed and read back {rows} rows in {}", path);
    Ok(())
}
