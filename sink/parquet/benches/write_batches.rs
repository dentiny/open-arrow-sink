//! End-to-end filesystem benchmark. Data generation, metadata inspection and
//! cleanup are outside the timed region; open, write and commit are included.
use std::{sync::Arc, time::Instant};

use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use open_arrow_sink_core::{Ordering, Sink, WriteOptions};
use open_arrow_sink_parquet::{ParquetDirectorySink, Url};
use parquet::arrow::ParquetRecordBatchStreamBuilder;

fn main() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let rows = 262_144;
        let batch = RecordBatch::try_from_iter([
            (
                "id",
                Arc::new(Int64Array::from_iter_values(0..rows as i64)) as ArrayRef,
            ),
            (
                "text",
                Arc::new(StringArray::from_iter_values(
                    (0..rows).map(|i| format!("value-{:04}-payload", i % 4096)),
                )) as ArrayRef,
            ),
        ])
        .unwrap();
        println!("batch_rows,target_bytes,sample,milliseconds,bytes,row_groups");
        for target in [None, Some(256 * 1024 * 1024)] {
            for batch_rows in [64, 1024, 8192] {
                // One untimed warmup followed by five independent sessions.
                for sample in 0..=5 {
                    let temp = tempfile::tempdir().unwrap();
                    let directory = temp.path().join("output");
                    let start = Instant::now();
                    let mut writer = ParquetDirectorySink::default()
                        .open(
                            Url::from_directory_path(&directory).unwrap(),
                            batch.schema(),
                            WriteOptions {
                                ordering: Ordering::Unordered,
                                target_file_size: target,
                                ..Default::default()
                            },
                        )
                        .await
                        .unwrap();
                    for offset in (0..rows).step_by(batch_rows) {
                        writer
                            .write(batch.slice(offset, batch_rows.min(rows - offset)))
                            .await
                            .unwrap();
                    }
                    assert_eq!(writer.commit().await.unwrap().rows_written, rows as u64);
                    let elapsed = start.elapsed();
                    let path = directory.join("part-00000.parquet");
                    let bytes = tokio::fs::metadata(&path).await.unwrap().len();
                    let reader = ParquetRecordBatchStreamBuilder::new(
                        tokio::fs::File::open(path).await.unwrap(),
                    )
                    .await
                    .unwrap();
                    assert_eq!(reader.metadata().file_metadata().num_rows(), rows as i64);
                    if sample > 0 {
                        println!(
                            "{batch_rows},{},{sample},{:.3},{bytes},{}",
                            target.unwrap_or(0),
                            elapsed.as_secs_f64() * 1000.0,
                            reader.metadata().num_row_groups()
                        );
                    }
                }
            }
        }
    });
}
