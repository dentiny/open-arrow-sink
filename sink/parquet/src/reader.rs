//! Range-based Parquet metadata access through shared storage.
use bytes::Bytes;
use futures::future::BoxFuture;
use open_arrow_sink_core::{Error, Result, SchemaRef};
use open_arrow_sink_storage::{Storage, Url};
use parquet::{
    arrow::{
        ParquetRecordBatchStreamBuilder, arrow_reader::ArrowReaderOptions,
        async_reader::AsyncFileReader,
    },
    file::metadata::{ParquetMetaData, ParquetMetaDataReader},
};
use std::{ops::Range, sync::Arc};

struct Reader {
    storage: Storage,
    url: Url,
    size: u64,
}
impl AsyncFileReader for Reader {
    fn get_bytes(&mut self, range: Range<u64>) -> BoxFuture<'_, parquet::errors::Result<Bytes>> {
        Box::pin(async move {
            self.storage
                .read_range(&self.url, range)
                .await
                .map_err(crate::error::parquet_backend)
        })
    }
    fn get_metadata<'a>(
        &'a mut self,
        _: Option<&'a ArrowReaderOptions>,
    ) -> BoxFuture<'a, parquet::errors::Result<Arc<ParquetMetaData>>> {
        Box::pin(async move {
            let size = self.size;
            ParquetMetaDataReader::new()
                .load_and_finish(self, size)
                .await
                .map(Arc::new)
        })
    }
}
pub(crate) async fn schema(storage: Storage, url: Url) -> Result<SchemaRef> {
    let size = storage.stat(&url).await?.content_length();
    let reader = Reader { storage, url, size };
    Ok(ParquetRecordBatchStreamBuilder::new(reader)
        .await
        .map_err(Error::backend)?
        .schema()
        .clone())
}
