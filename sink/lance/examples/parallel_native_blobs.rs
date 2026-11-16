//! Eight physical writers ingest native logical blobs and publish one manifest.
use std::{num::NonZeroUsize, sync::Arc};

use arrow_array::{RecordBatch, UInt64Array};
use arrow_schema::{DataType, Field, Schema};
use futures::future::join_all;
use lance::{
    blob::{BlobArrayBuilder, blob_field},
    datatypes::BlobKind,
};
use open_arrow_sink_core::{BlobHandleConfig, Ordering, ParallelSink, WriteOptions};
use open_arrow_sink_lance::{LanceDatasetSink, Storage};
use open_arrow_sink_storage::{BlobDownloader, ObjectPath, opendal};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    const WRITERS: usize = 8;
    const ROWS_PER_WRITER: usize = 3;
    // Independent operators demonstrate source/destination credentials and namespaces.
    let source = Storage::new(
        opendal::Operator::new(opendal::services::Memory::default())?,
        "memory://sources/".parse()?,
    )?;
    let destination = Storage::new(
        opendal::Operator::new(opendal::services::Memory::default())?,
        "memory://destination/".parse()?,
    )?;
    let packed_source = source.url(&ObjectPath::parse("packed.bin")?)?;
    let dedicated_source = source.url(&ObjectPath::parse("dedicated.bin")?)?;
    source
        .write(&source.path(&packed_source)?, b"pack".to_vec().into())
        .await?;
    source
        .write(&source.path(&dedicated_source)?, b"large".to_vec().into())
        .await?;
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::UInt64, false),
        blob_field("blob", false),
    ]));
    // The downloader is always part of blob ingestion. These limits are shared
    // by all physical writers; SDK encoding has a separate per-writer budget.
    let downloader = Arc::new(
        BlobDownloader::new(
            NonZeroUsize::new(16).unwrap(),
            NonZeroUsize::new(64 * 1024 * 1024).unwrap(),
        )?
        .with_download_limits(
            NonZeroUsize::new(1024 * 1024).unwrap(),
            NonZeroUsize::new(64 * 1024).unwrap(),
        )?,
    );
    let sink = LanceDatasetSink::new(destination.clone())
        .with_source_storage(source.clone())
        .with_blob_downloader(downloader.clone())
        .with_encoding_buffer_size(NonZeroUsize::new(4 * 1024 * 1024).unwrap());
    let url = destination.url(&ObjectPath::parse("dataset/")?)?;
    // Field updates also support the non-exhaustive options API.
    #[allow(clippy::field_reassign_with_default)]
    let options = {
        let mut options = WriteOptions::default();
        options.ordering = Ordering::Unordered;
        options.blob_handle = Some(BlobHandleConfig {
            inline_size_threshold: Some(1),
            dedicated_size_threshold: NonZeroUsize::new(4),
            ..Default::default()
        });
        options
    };
    // Each physical writer receives a batch. This exercises all eight writers,
    // independently of an unordered dispatcher's scheduling or batch availability.
    let mut batches = Vec::new();
    for worker in 0..WRITERS {
        let mut blobs = BlobArrayBuilder::new(ROWS_PER_WRITER);
        blobs.push_bytes(b"x")?;
        blobs.push_uri(packed_source.as_str())?;
        blobs.push_uri(dedicated_source.as_str())?;
        let start = (worker * ROWS_PER_WRITER) as u64;
        batches.push(RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(UInt64Array::from_iter_values(
                    start..start + ROWS_PER_WRITER as u64,
                )),
                blobs.finish()?,
            ],
        )?);
    }
    let session = sink
        .open_writers(
            url.clone(),
            schema.clone(),
            options,
            NonZeroUsize::new(WRITERS).unwrap(),
        )
        .await?;
    let finished = join_all(session.writers.into_iter().zip(batches).map(
        |(mut writer, batch)| async move {
            let result = async {
                writer.write(batch).await?;
                writer.finish().await
            }
            .await;
            // Retain ownership until every finish result is known and publication ends.
            (writer, result)
        },
    ))
    .await;
    let mut writers = Vec::new();
    let mut outputs = Vec::new();
    let mut failure = None;
    for (writer, result) in finished {
        writers.push(writer);
        match result {
            Ok(output) => outputs.push(output),
            Err(error) => {
                failure.get_or_insert(error);
            }
        }
    }
    if let Some(error) = failure {
        return Err(error.into());
    }
    // Only publish after all writers finish successfully.
    let committed = session.committer.commit(outputs).await;
    drop(writers);
    let summary = committed?;
    assert_eq!(downloader.buffered_bytes(), 0);
    source.delete(&source.path(&packed_source)?).await?;
    source.delete(&source.path(&dedicated_source)?).await?;

    let dataset = Arc::new(sink.dataset_builder(url)?.load().await?);
    let rows = WRITERS * ROWS_PER_WRITER;
    assert_eq!(summary.rows_written, rows as u64);
    assert_eq!(dataset.count_rows(None).await?, rows);
    assert_eq!(dataset.get_fragments().len(), WRITERS);
    assert_eq!(dataset.version().version, 1);
    let output = dataset.scan().try_into_batch().await?;
    let mut ids = output
        .column(0)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or("unexpected id type")?
        .values()
        .to_vec();
    ids.sort_unstable();
    assert_eq!(ids, (0..rows as u64).collect::<Vec<_>>());
    let indices = (0..rows as u64).collect::<Vec<_>>();
    let mut counts = [0; 3];
    for blob in dataset.take_blobs_by_indices(&indices, "blob").await? {
        let blob = blob.ok_or("unexpected null blob")?;
        let (index, expected): (_, &[u8]) = match blob.kind() {
            BlobKind::Inline => (0, b"x"),
            BlobKind::Packed => (1, b"pack"),
            BlobKind::Dedicated => (2, b"large"),
            other => return Err(format!("unexpected blob layout: {other:?}").into()),
        };
        assert_eq!(blob.read().await?.as_ref(), expected);
        counts[index] += 1;
    }
    assert_eq!(counts, [WRITERS; 3]);
    println!(
        "{WRITERS} physical writers committed {rows} owned blobs in one data manifest; all three layouts read back after deleting sources"
    );
    Ok(())
}
