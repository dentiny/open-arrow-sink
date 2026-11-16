//! Backend-independent helpers for successful write sessions.
use open_arrow_sink_core::{RecordBatch, Sink, WriteOptions, WriteSummary};

/// Commit a nonempty sequence of batches and verify the schema and row count.
/// Empty commits and deliberate failure scenarios should use the contract suite
/// or operate on the writer directly.
pub async fn write_batches<S: Sink + ?Sized>(
    sink: &S,
    request: S::Request,
    batches: &[RecordBatch],
    options: WriteOptions,
) -> WriteSummary {
    let schema = batches
        .first()
        .expect("provide at least one batch")
        .schema();
    let mut writer = sink.open(request, schema.clone(), options).await.unwrap();
    assert_eq!(writer.schema(), &schema);
    let mut rows = 0;
    for batch in batches {
        rows += batch.num_rows() as u64;
        writer.write(batch.clone()).await.unwrap();
    }
    let summary = writer.commit().await.unwrap();
    assert_eq!(summary.rows_written, rows);
    summary
}
