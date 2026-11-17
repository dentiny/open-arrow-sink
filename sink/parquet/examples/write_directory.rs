use std::sync::Arc;

use arrow_array::{ArrayRef, Int64Array, RecordBatch};
use open_arrow_sink_core::{Ordering, SinkExt, WriteOptions};
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
    let summary = ParquetDirectorySink::default()
        .write_all(
            directory,
            batch.schema(),
            WriteOptions::default().with_ordering(Ordering::Unordered),
            futures::stream::iter([Ok::<_, std::io::Error>(batch)]),
        )
        .await?;
    println!("Committed {} rows", summary.rows_committed);
    Ok(())
}
