use std::{num::NonZeroUsize, sync::Arc};

use arrow_array::{ArrayRef, Int64Array, RecordBatch};
use bytes::Bytes;
use futures::TryStreamExt;
use object_store::{ObjectStore, ObjectStoreExt, memory::InMemory, path::Path};
use open_arrow_sink_core::{DataWriter, Ordering, Sink, WriteOptions, Writer};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use crate::{ParquetDirectorySink, Url, destination::Destination, writer::ParquetDirectoryWriter};

fn batch() -> RecordBatch {
    RecordBatch::try_from_iter([("id", Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef)])
        .unwrap()
}

fn options() -> WriteOptions {
    WriteOptions {
        ordering: Ordering::Unordered,
        target_file_size: Some(1),
        ..Default::default()
    }
}

fn destination(store: &Arc<InMemory>) -> Destination {
    Destination::Remote {
        store: store.clone(),
        prefix: Path::from("new output"),
    }
}

async fn open(
    store: &Arc<InMemory>,
    options: WriteOptions,
    count: usize,
) -> Vec<ParquetDirectoryWriter> {
    let schema = batch().schema();
    let properties = crate::compression::writer_properties(&schema, &options.compression).unwrap();
    ParquetDirectorySink
        .create_writers_at(
            destination(store),
            schema,
            options,
            NonZeroUsize::new(count).unwrap(),
            properties,
        )
        .await
        .unwrap()
}

async fn paths(store: &Arc<InMemory>) -> Vec<Path> {
    store
        .list(None)
        .map_ok(|object| object.location)
        .try_collect()
        .await
        .unwrap()
}

async fn read(store: &Arc<InMemory>, index: usize) -> Vec<RecordBatch> {
    let bytes = store
        .get(&Path::from(format!("new output/part-{index:05}.parquet")))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    ParquetRecordBatchReaderBuilder::try_new(bytes)
        .unwrap()
        .build()
        .unwrap()
        .map(|batch| batch.unwrap())
        .collect()
}

#[tokio::test]
async fn remote_parallel_rolling_round_trips_and_abort_cleans_finished_shards() {
    let store = Arc::new(InMemory::new());
    let writers = open(&store, options(), 3).await;
    for (index, mut writer) in writers.into_iter().enumerate() {
        DataWriter::write(&mut writer, batch()).await.unwrap();
        DataWriter::write(&mut writer, batch()).await.unwrap();
        assert_eq!(DataWriter::finish(&mut writer).await.unwrap(), 6);
        assert_eq!(read(&store, index).await, vec![batch()]);
        assert_eq!(read(&store, index + 3).await, vec![batch()]);
        DataWriter::abort(Box::new(writer)).await.unwrap();
    }
    assert!(paths(&store).await.is_empty());
}

#[tokio::test]
async fn remote_commit_and_empty_commit_are_readable() {
    for write in [false, true] {
        let store = Arc::new(InMemory::new());
        let mut writer = open(&store, options(), 1).await.pop().unwrap();
        if write {
            Writer::write(&mut writer, batch()).await.unwrap();
        }
        assert_eq!(
            Writer::commit(Box::new(writer)).await.unwrap().rows_written,
            if write { 3 } else { 0 }
        );
        assert_eq!(
            read(&store, 0).await,
            if write { vec![batch()] } else { vec![] }
        );
        assert!(destination(&store).reserve().await.is_err());
    }
}

#[tokio::test]
async fn remote_reservation_is_exclusive() {
    let store = Arc::new(InMemory::new());
    let first = destination(&store);
    let second = first.clone();
    let (a, b) = tokio::join!(first.reserve(), second.reserve());
    assert_ne!(a.is_ok(), b.is_ok());
}

#[tokio::test]
async fn remote_existing_objects_are_preserved() {
    for name in [
        "new output",
        "new output/part-00000.parquet",
        "new output/keep",
    ] {
        let store = Arc::new(InMemory::new());
        store
            .put(&Path::from(name), Bytes::from_static(b"keep").into())
            .await
            .unwrap();
        assert!(destination(&store).reserve().await.is_err());
        assert_eq!(
            store
                .get(&Path::from(name))
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap(),
            "keep"
        );
        assert_eq!(paths(&store).await.len(), 1);
    }
}

#[tokio::test]
async fn remote_abort_preserves_unrelated_objects_and_cleanup_policy() {
    for cleanup_on_error in [true, false] {
        let store = Arc::new(InMemory::new());
        let mut writer = open(
            &store,
            WriteOptions {
                cleanup_on_error,
                ..options()
            },
            1,
        )
        .await
        .pop()
        .unwrap();
        Writer::write(&mut writer, batch()).await.unwrap();
        Writer::write(&mut writer, batch()).await.unwrap();
        store
            .put(
                &Path::from("new output/keep"),
                Bytes::from_static(b"keep").into(),
            )
            .await
            .unwrap();
        Writer::abort(Box::new(writer)).await.unwrap();
        assert_eq!(
            paths(&store).await.len(),
            if cleanup_on_error { 2 } else { 4 }
        );
        assert_eq!(
            store
                .get(&Path::from("new output/keep"))
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap(),
            "keep"
        );
    }
}

#[tokio::test]
async fn failed_remote_write_is_terminal_and_abort_removes_owned_output() {
    let store = Arc::new(InMemory::new());
    let mut writer = open(&store, options(), 1).await.pop().unwrap();
    let wrong =
        RecordBatch::try_from_iter([("wrong", Arc::new(Int64Array::from(vec![1])) as ArrayRef)])
            .unwrap();
    assert!(Writer::write(&mut writer, wrong).await.is_err());
    assert!(Writer::write(&mut writer, batch()).await.is_err());
    assert!(Writer::commit(Box::new(writer)).await.is_err());
    assert!(paths(&store).await.is_empty());
}

#[tokio::test]
async fn remote_roll_collision_is_never_deleted_by_abort() {
    let store = Arc::new(InMemory::new());
    let mut writer = open(&store, options(), 1).await.pop().unwrap();
    Writer::write(&mut writer, batch()).await.unwrap();
    let collision = Path::from("new output/part-00001.parquet");
    store
        .put(&collision, Bytes::from_static(b"keep").into())
        .await
        .unwrap();
    assert!(Writer::write(&mut writer, batch()).await.is_err());
    Writer::abort(Box::new(writer)).await.unwrap();
    assert_eq!(
        store.get(&collision).await.unwrap().bytes().await.unwrap(),
        "keep"
    );
}

#[tokio::test]
async fn public_url_entry_points_and_validation() {
    let mut writer = ParquetDirectorySink
        .open(
            "memory:///new%20output".parse().unwrap(),
            batch().schema(),
            options(),
        )
        .await
        .unwrap();
    writer.write(batch()).await.unwrap();
    assert_eq!(writer.commit().await.unwrap().rows_written, 3);
    for url in [
        "memory:///",
        "memory:///output?x=1",
        "memory:///output#x",
        "unsupported://host/output",
    ] {
        assert!(
            ParquetDirectorySink
                .open(url.parse().unwrap(), batch().schema(), options())
                .await
                .is_err()
        );
    }
    let Destination::Remote { prefix, .. } =
        Destination::from_url("memory:///new%20output/".parse().unwrap()).unwrap()
    else {
        panic!()
    };
    assert_eq!(prefix.as_ref(), "new output");
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("雪 space");
    let url = Url::from_directory_path(&path).unwrap();
    assert!(
        ParquetDirectorySink
            .open(url.clone(), batch().schema(), WriteOptions::default())
            .await
            .is_err()
    );
    assert!(!path.exists());
    let writer = ParquetDirectorySink
        .open(url.clone(), batch().schema(), options())
        .await
        .unwrap();
    assert!(
        ParquetDirectorySink
            .open(url, batch().schema(), options())
            .await
            .is_err()
    );
    writer.abort().await.unwrap();
    assert!(!path.exists());
}

#[tokio::test]
async fn remote_multipart_upload_crosses_part_boundary() {
    use arrow_array::StringArray;
    let store = Arc::new(InMemory::new());
    let large = RecordBatch::try_from_iter([(
        "value",
        Arc::new(StringArray::from(vec!["x".repeat(6 * 1024 * 1024)])) as ArrayRef,
    )])
    .unwrap();
    let properties =
        crate::compression::writer_properties(&large.schema(), &options().compression).unwrap();
    let mut writer = ParquetDirectorySink
        .create_writers_at(
            destination(&store),
            large.schema(),
            options(),
            NonZeroUsize::new(1).unwrap(),
            properties,
        )
        .await
        .unwrap()
        .pop()
        .unwrap();
    Writer::write(&mut writer, large.clone()).await.unwrap();
    Writer::commit(Box::new(writer)).await.unwrap();
    assert_eq!(read(&store, 0).await, vec![large]);
}
