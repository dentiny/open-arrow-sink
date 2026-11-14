use std::{path::PathBuf, sync::Arc};

use arrow_array::{ArrayRef, Int64Array, RecordBatch};
use lance::Dataset;
use open_arrow_sink_core::{Ordering, Sink, WriteOptions};
use open_arrow_sink_lance::LanceDatasetSink;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let path = PathBuf::from(
        std::env::args()
            .nth(1)
            .ok_or("pass a new dataset directory")?,
    );
    let batch = RecordBatch::try_from_iter([(
        "value",
        Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef,
    )])?;
    let mut writer = LanceDatasetSink
        .open(
            path.clone(),
            batch.schema(),
            WriteOptions {
                ordering: Ordering::Unordered,
                ..WriteOptions::default()
            },
        )
        .await?;
    writer.write(batch.clone()).await?;
    writer.write(batch).await?;
    let summary = writer.commit().await?;
    let dataset = Dataset::open(path.to_str().ok_or("path must be UTF-8")?).await?;
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
    assert_eq!(summary.rows_written, 6);
    assert_eq!(rows, 6);
    println!("committed and read back {rows} rows in {}", path.display());
    Ok(())
}
