use futures::{Stream, TryStreamExt, pin_mut};

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
    let result = async {
        while let Some(batch) = stream.try_next().await? {
            writer.write(batch).await?;
        }
        Ok(())
    }
    .await;
    if let Err(error) = result {
        let _ = writer.abort().await;
        return Err(error);
    }
    writer.commit().await
}
