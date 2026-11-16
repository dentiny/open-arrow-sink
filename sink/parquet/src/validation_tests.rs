use super::*;
use arrow_array::{ArrayRef, Int64Array, RecordBatch};
use open_arrow_sink_storage::opendal::{self, OperationContext, raw::*};
use parquet::arrow::ArrowWriter;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

#[derive(Debug, Default)]
struct Observations {
    active: AtomicUsize,
    peak: AtomicUsize,
    calls: AtomicUsize,
}
struct Active(Arc<Observations>);
impl Drop for Active {
    fn drop(&mut self) {
        self.0.active.fetch_sub(1, Ordering::SeqCst);
    }
}
#[derive(Debug)]
struct ObserveLayer(Arc<Observations>);
impl Layer for ObserveLayer {
    fn apply_service(&self, inner: Servicer) -> Servicer {
        Arc::new(ObservedService {
            inner,
            observations: self.0.clone(),
        })
    }
}
#[derive(Debug)]
struct ObservedService {
    inner: Servicer,
    observations: Arc<Observations>,
}
impl Service for ObservedService {
    type Reader = oio::Reader;
    type Writer = oio::Writer;
    type Lister = oio::Lister;
    type Deleter = oio::Deleter;
    type Copier = oio::Copier;
    type Composer = oio::Composer;

    fn info(&self) -> ServiceInfo {
        self.inner.info()
    }
    async fn create_dir(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpCreateDir,
    ) -> opendal::Result<RpCreateDir> {
        self.inner.create_dir(ctx, path, args).await
    }
    fn write(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpWrite,
    ) -> opendal::Result<Self::Writer> {
        self.inner.write(ctx, path, args)
    }
    fn delete(&self, ctx: &OperationContext) -> opendal::Result<Self::Deleter> {
        self.inner.delete(ctx)
    }
    fn copy(
        &self,
        ctx: &OperationContext,
        from: &str,
        to: &str,
        args: OpCopy,
    ) -> opendal::Result<Self::Copier> {
        self.inner.copy(ctx, from, to, args)
    }
    async fn rename(
        &self,
        ctx: &OperationContext,
        from: &str,
        to: &str,
        args: OpRename,
    ) -> opendal::Result<RpRename> {
        self.inner.rename(ctx, from, to, args).await
    }
    async fn presign(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpPresign,
    ) -> opendal::Result<RpPresign> {
        self.inner.presign(ctx, path, args).await
    }
    fn capability(&self) -> opendal::Capability {
        self.inner.capability()
    }
    fn read(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpRead,
    ) -> opendal::Result<Self::Reader> {
        self.inner.read(ctx, path, args)
    }
    fn list(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpList,
    ) -> opendal::Result<Self::Lister> {
        self.inner.list(ctx, path, args)
    }
    async fn stat(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpStat,
    ) -> opendal::Result<RpStat> {
        if path.ends_with(".parquet") {
            self.observations.calls.fetch_add(1, Ordering::SeqCst);
            let active = self.observations.active.fetch_add(1, Ordering::SeqCst) + 1;
            self.observations.peak.fetch_max(active, Ordering::SeqCst);
            let _active = Active(self.observations.clone());
            // Let every scheduled schema check reach an observable I/O boundary.
            tokio::task::yield_now().await;
            self.inner.stat(ctx, path, args).await
        } else {
            self.inner.stat(ctx, path, args).await
        }
    }
}
fn parquet_bytes(batch: &RecordBatch) -> Vec<u8> {
    let mut writer = ArrowWriter::try_new(Vec::new(), batch.schema(), None).unwrap();
    writer.write(batch).unwrap();
    writer.into_inner().unwrap()
}
#[tokio::test]
async fn append_checks_all_footers_with_bounded_io() {
    let base: Url = "memory:///warehouse/".parse().unwrap();
    let storage = Storage::new(
        opendal::Operator::new(opendal::services::Memory::default()).unwrap(),
        base.clone(),
    )
    .unwrap();
    let directory = storage.url("output/").unwrap();
    let batch =
        RecordBatch::try_from_iter([("value", Arc::new(Int64Array::from(vec![1])) as ArrayRef)])
            .unwrap();
    let bytes = parquet_bytes(&batch);
    for file in 0..96 {
        storage
            .write(
                &directory.join(&format!("part-{file:05}.parquet")).unwrap(),
                bytes.clone().into(),
            )
            .await
            .unwrap();
    }
    // Non-Parquet files and nested datasets remain outside the append contract.
    storage
        .write(&directory.join("notes.txt").unwrap(), "ignored".into())
        .await
        .unwrap();
    storage
        .write(
            &directory.join("nested/ignored.parquet").unwrap(),
            "ignored".into(),
        )
        .await
        .unwrap();
    let observations = Arc::new(Observations::default());
    let observed = Storage::new(
        storage.operator().layer(ObserveLayer(observations.clone())),
        base,
    )
    .unwrap();
    validate_append_directory(&observed, &directory, &batch.schema())
        .await
        .unwrap();
    assert_eq!(observations.calls.load(Ordering::SeqCst), 96);
    let peak = observations.peak.load(Ordering::SeqCst);
    assert!(peak > 1 && peak <= 32, "peak footer I/O: {peak}");
    assert_eq!(observations.active.load(Ordering::SeqCst), 0);

    // More files than fit in the window must still reject incompatible schemas.
    let wrong = RecordBatch::try_from_iter([(
        "different",
        Arc::new(Int64Array::from(vec![1])) as ArrayRef,
    )])
    .unwrap();
    let last = directory.join("zz-last.parquet").unwrap();
    storage
        .write(&last, parquet_bytes(&wrong).into())
        .await
        .unwrap();
    assert!(matches!(
        validate_append_directory(&observed, &directory, &batch.schema()).await,
        Err(open_arrow_sink_core::Error::SchemaMismatch)
    ));
    assert_eq!(observations.active.load(Ordering::SeqCst), 0);
    storage
        .write(&last, "invalid parquet footer".into())
        .await
        .unwrap();
    assert!(matches!(
        validate_append_directory(&observed, &directory, &batch.schema()).await,
        Err(open_arrow_sink_core::Error::Backend(_))
    ));
    assert_eq!(observations.active.load(Ordering::SeqCst), 0);
}
