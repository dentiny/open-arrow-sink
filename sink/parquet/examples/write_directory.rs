use std::sync::Arc;

use arrow_array::{ArrayRef, Int64Array, RecordBatch};
use open_arrow_sink_core::{Ordering, Sink, WriteOptions};
use open_arrow_sink_parquet::{ParquetDirectorySink, Url};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let argument = std::env::args()
        .nth(1)
        .ok_or("usage: write_directory NEW_DIRECTORY_OR_URL")?;
    let directory = match Url::parse(&argument) {
        Ok(url) => url,
        Err(_) => Url::from_directory_path(std::path::absolute(argument)?)
            .map_err(|_| "invalid directory")?,
    };
    let batch = RecordBatch::try_from_iter([(
        "value",
        Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef,
    )])?;
    let mut writer = ParquetDirectorySink
        .open(
            directory,
            batch.schema(),
            WriteOptions {
                ordering: Ordering::Unordered,
                ..WriteOptions::default()
            },
        )
        .await?;
    writer.write(batch).await?;
    let summary = writer.commit().await?;
    println!("Wrote {} rows", summary.rows_written);
    Ok(())
}
