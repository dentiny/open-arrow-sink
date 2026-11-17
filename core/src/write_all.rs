use futures::{Stream, StreamExt, pin_mut};

use crate::{CommitSummary, RecordBatch, Result, SchemaRef, Sink, WriteOptions};

pub(crate) async fn write_all<T, S>(
    sink: &T,
    request: T::Request,
    schema: SchemaRef,
    options: WriteOptions,
    stream: S,
) -> Result<CommitSummary>
where
    T: Sink,
    S: Stream<Item = Result<RecordBatch>> + Send,
{
    let mut writer = sink.open(request, schema, options).await?;
    pin_mut!(stream);
    while let Some(batch) = stream.next().await {
        let batch = match batch {
            Ok(batch) => batch,
            Err(error) => {
                let _ = writer.abort().await;
                return Err(error);
            }
        };
        if let Err(error) = writer.write(batch).await {
            let _ = writer.abort().await;
            return Err(error);
        }
    }
    writer.commit().await
}
