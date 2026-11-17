use futures::{Stream, StreamExt, pin_mut};

use crate::{Error, RecordBatch, SchemaRef, Sink, WriteOptions};

/// Input accepted by the writer, returned only after commit succeeds.
/// Counts precede backend filtering or transformation; they are not a count of
/// persisted rows. An empty batch contributes one batch and zero rows.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct WriteSummary {
    pub batches: u64,
    pub rows: u64,
}

/// Retains the original input or sink error, even if abort also fails.
/// No dependency on the input producer's library or error type is required.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum WriteAllError<E> {
    #[error("input stream failed: {0}")]
    Input(#[source] E),
    #[error(transparent)]
    Sink(#[from] Error),
}

pub(crate) async fn write_all<T, S, E, F>(
    sink: &T,
    request: T::Request,
    schema: SchemaRef,
    options: WriteOptions,
    batches: S,
    mut progress: F,
) -> Result<WriteSummary, WriteAllError<E>>
where
    T: Sink,
    S: Stream<Item = Result<RecordBatch, E>> + Send,
    E: Send,
    F: FnMut(usize) + Send,
{
    let mut writer = sink.open(request, schema, options).await?;
    let mut summary = WriteSummary::default();
    pin_mut!(batches);
    while let Some(batch) = batches.next().await {
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
        progress(rows);
    }
    writer.commit().await?;
    Ok(summary)
}
