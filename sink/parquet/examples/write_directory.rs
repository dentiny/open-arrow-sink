use std::{path::PathBuf, sync::Arc};

use arrow_array::{ArrayRef, Int64Array, RecordBatch};
use open_arrow_sink_core::{Ordering, Sink, WriteOptions};
use open_arrow_sink_parquet::ParquetDirectorySink;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let directory = PathBuf::from(
        std::env::args()
            .nth(1)
            .ok_or("usage: write_directory NEW_DIRECTORY")?,
    );
    let batch = RecordBatch::try_from_iter([(
        "value",
        Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef,
    )])?;
    let mut writer = ParquetDirectorySink
        .open(
            directory.clone(),
            batch.schema(),
            WriteOptions {
                ordering: Ordering::Unordered,
            },
        )
        .await?;
    writer.write(batch.clone()).await?;
    let summary = writer.commit().await?;
    let mut reader = ParquetRecordBatchReaderBuilder::try_new(std::fs::File::open(
        directory.join("part-00000.parquet"),
    )?)?
    .build()?;
    assert_eq!(reader.next().transpose()?.as_ref(), Some(&batch));
    assert!(reader.next().is_none());
    println!("Wrote and read back {} rows", summary.rows_written);
    Ok(())
}
