use bytes::Bytes;
use futures::future::BoxFuture;
use open_arrow_sink_core::Result;
use open_arrow_sink_storage::StorageWriter;
use parquet::arrow::async_writer::AsyncFileWriter;

pub(crate) struct FileWriter(pub(crate) StorageWriter);
impl FileWriter {
    pub(crate) async fn abort(&mut self) -> Result<()> {
        self.0.abort().await
    }
}
impl AsyncFileWriter for FileWriter {
    fn write(&mut self, bytes: Bytes) -> BoxFuture<'_, parquet::errors::Result<()>> {
        Box::pin(async move {
            self.0
                .write(bytes)
                .await
                .map_err(crate::error::parquet_backend)
        })
    }
    fn complete(&mut self) -> BoxFuture<'_, parquet::errors::Result<()>> {
        Box::pin(async move {
            self.0
                .close()
                .await
                .map(|_| ())
                .map_err(crate::error::parquet_backend)
        })
    }
}
