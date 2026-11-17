use futures::{Stream, StreamExt, pin_mut};

use crate::{CommitSummary, RecordBatch, SchemaRef, Sink, WriteAllError, WriteOptions};

pub(crate) async fn write_all<T, S, E>(
    sink: &T,
    request: T::Request,
    schema: SchemaRef,
    options: WriteOptions,
    stream: S,
) -> Result<CommitSummary, WriteAllError<E>>
where
    T: Sink,
    S: Stream<Item = Result<RecordBatch, E>> + Send,
    E: Send,
{
    let mut writer = sink.open(request, schema, options).await?;
    pin_mut!(stream);
    while let Some(batch) = stream.next().await {
        let batch = match batch {
            Ok(batch) => batch,
            Err(error) => {
                let _ = writer.abort().await;
                return Err(WriteAllError::Input(error));
            }
        };
        if let Err(error) = writer.write(batch).await {
            let _ = writer.abort().await;
            return Err(WriteAllError::Sink(error));
        }
    }
    writer.commit().await.map_err(WriteAllError::Sink)
}
