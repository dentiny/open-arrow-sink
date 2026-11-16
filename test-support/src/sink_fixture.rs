//! Backend-independent helpers for successful write sessions.
use open_arrow_sink_core::{RecordBatch, Sink, WriteOptions};

/// Commit a nonempty sequence of batches and verify the writer schema.
/// Empty commits and deliberate failure scenarios should use the contract suite
/// or operate on the writer directly.
pub async fn write_batches<S: Sink + ?Sized>(
    sink: &S,
    request: S::Request,
    batches: &[RecordBatch],
    options: WriteOptions,
) {
    let schema = batches
        .first()
        .expect("provide at least one batch")
        .schema();
    let mut writer = sink.open(request, schema.clone(), options).await.unwrap();
    assert_eq!(writer.schema(), &schema);
    for batch in batches {
        writer.write(batch.clone()).await.unwrap();
    }
    writer.commit().await.unwrap();
}
