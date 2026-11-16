use std::{num::NonZeroUsize, sync::Arc};

use arrow_array::{ArrayRef, Int64Array, RecordBatch};
use open_arrow_sink_core::{Ordering, ParallelSink, WriteMode, WriteOptions};
use open_arrow_sink_parquet::{ParquetDirectorySink, Storage};
use open_arrow_sink_storage::opendal;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

fn batch() -> RecordBatch {
    RecordBatch::try_from_iter([(
        "value",
        Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef,
    )])
    .unwrap()
}
// Field updates also work after the public options become non-exhaustive.
#[allow(clippy::field_reassign_with_default)]
fn options(mode: WriteMode) -> WriteOptions {
    let mut options = WriteOptions::default();
    options.ordering = Ordering::Unordered;
    options.mode = mode;
    options
}
fn memory() -> Storage {
    Storage::new(
        opendal::Operator::new(opendal::services::Memory::default()).unwrap(),
        "memory:///warehouse/".parse().unwrap(),
    )
    .unwrap()
}

#[tokio::test]
async fn idle_workers_do_not_open_files_or_leave_empty_parts() {
    let storage = memory();
    let sink = ParquetDirectorySink::new(storage.clone());
    let url = storage.url("sparse/").unwrap();
    let mut session = sink
        .open_writers(
            url.clone(),
            batch().schema(),
            options(WriteMode::Create),
            NonZeroUsize::new(8).unwrap(),
        )
        .await
        .unwrap();
    let before = storage.list(&url).await.unwrap();
    assert_eq!(before.len(), 1);
    assert!(before[0].path().ends_with(".open-arrow-sink"));
    session.writers[3].write(batch().slice(0, 0)).await.unwrap();
    session.writers[5].write(batch()).await.unwrap();
    let mut outputs = Vec::new();
    for writer in &mut session.writers {
        outputs.push(writer.finish().await.unwrap());
    }
    assert_eq!(
        session
            .committer
            .commit(outputs)
            .await
            .unwrap()
            .rows_written,
        3
    );
    let files = storage.list(&url).await.unwrap();
    assert_eq!(files.len(), 1);
    assert!(files[0].path().ends_with("part-00005.parquet"));
    let reader =
        ParquetRecordBatchReaderBuilder::try_new(storage.read(&files[0]).await.unwrap()).unwrap();
    assert_eq!(reader.metadata().file_metadata().num_rows(), 3);
}

#[tokio::test]
async fn empty_create_has_one_readable_footer_and_empty_append_creates_nothing() {
    let storage = memory();
    let sink = ParquetDirectorySink::new(storage.clone());
    let url = storage.url("empty/").unwrap();
    for mode in [WriteMode::Create, WriteMode::Append] {
        let mut session = sink
            .open_writers(
                url.clone(),
                batch().schema(),
                options(mode),
                NonZeroUsize::new(8).unwrap(),
            )
            .await
            .unwrap();
        session.writers[4].write(batch().slice(0, 0)).await.unwrap();
        let mut outputs = Vec::new();
        for writer in &mut session.writers {
            outputs.push(writer.finish().await.unwrap());
        }
        assert_eq!(
            session
                .committer
                .commit(outputs)
                .await
                .unwrap()
                .rows_written,
            0
        );
        let files = storage.list(&url).await.unwrap();
        assert_eq!(files.len(), 1);
        assert!(files[0].path().ends_with("part-00000.parquet"));
        let reader =
            ParquetRecordBatchReaderBuilder::try_new(storage.read(&files[0]).await.unwrap())
                .unwrap();
        assert_eq!(reader.schema(), &batch().schema());
        assert_eq!(reader.metadata().file_metadata().num_rows(), 0);
    }
}

#[tokio::test]
async fn aborting_idle_workers_obeys_cleanup_policy() {
    for cleanup in [true, false] {
        let storage = memory();
        let sink = ParquetDirectorySink::new(storage.clone());
        let url = storage.url("abort/").unwrap();
        let mut config = options(WriteMode::Create);
        config.cleanup_on_error = cleanup;
        let session = sink
            .open_writers(
                url.clone(),
                batch().schema(),
                config,
                NonZeroUsize::new(8).unwrap(),
            )
            .await
            .unwrap();
        for writer in session.writers {
            writer.abort().await.unwrap();
        }
        session.committer.abort().await.unwrap();
        assert_eq!(storage.list(&url).await.unwrap().is_empty(), cleanup);
    }
}
