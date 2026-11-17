//! Range-based Parquet metadata access through shared storage.
use bytes::Bytes;
use futures::future::BoxFuture;
use open_arrow_sink_core::{Error, Result, SchemaRef};
use open_arrow_sink_storage::{ObjectPath, Storage};
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
    path: ObjectPath,
    size: u64,
}
impl AsyncFileReader for Reader {
    fn get_bytes(&mut self, range: Range<u64>) -> BoxFuture<'_, parquet::errors::Result<Bytes>> {
        Box::pin(async move {
            self.storage
                .read_range(&self.path, range)
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
pub(crate) async fn schema(storage: Storage, path: ObjectPath) -> Result<SchemaRef> {
    let size = storage.metadata(&path).await?.content_length();
    let reader = Reader {
        storage,
        path,
        size,
    };
    Ok(ParquetRecordBatchStreamBuilder::new(reader)
        .await
        .map_err(Error::backend)?
        .schema()
        .clone())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn absent_parquet_metadata_returns_not_found() {
        let storage = open_arrow_sink_test_support::memory_storage("memory:///warehouse/");
        let path = ObjectPath::parse("missing.parquet").unwrap();
        assert!(matches!(
            schema(storage, path).await,
            Err(Error::NotFound(_))
        ));
    }
}
