use futures::{Stream, StreamExt, pin_mut};

use crate::{RecordBatch, SchemaRef, Sink, WriteAllError, WriteOptions};

/// Input accepted by the writer, returned only after commit succeeds.
/// Counts precede backend filtering or transformation; they are not a count of
/// persisted rows. An empty batch contributes one batch and zero rows.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct WriteSummary {
    pub batches: u64,
    pub rows: u64,
}

pub(crate) async fn write_all<T, S, E>(
    sink: &T,
    request: T::Request,
    schema: SchemaRef,
    options: WriteOptions,
    stream: S,
) -> Result<WriteSummary, WriteAllError<E>>
where
    T: Sink,
    S: Stream<Item = Result<RecordBatch, E>> + Send,
    E: Send,
{
    let mut writer = sink.open(request, schema, options).await?;
    let mut summary = WriteSummary::default();
    pin_mut!(stream);
    while let Some(batch) = stream.next().await {
        let batch = match batch {
            Ok(batch) => batch,
            Err(error) => {
                let _ = writer.abort().await;
                return Err(WriteAllError::Input(error));
            }
        };
        let rows = batch.num_rows();
        if let Err(error) = writer.write(batch).await {
            let _ = writer.abort().await;
            return Err(WriteAllError::Sink(error));
        }
        summary.batches += 1;
        summary.rows += rows as u64;
    }
    writer.commit().await?;
    Ok(summary)
}
