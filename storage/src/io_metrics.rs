//! Successful OpenDAL payload transfers, independent of publication and cleanup.
use open_arrow_sink_core::IoMetrics;
use opendal::raw::oio::{Read, ReadStream, Write};
use opendal::{Buffer, Capability, Metadata, Result, raw::*};
use std::sync::Arc;

/// Count bytes returned by reads and accepted by writes on any OpenDAL backend.
/// Install outside retry layers to avoid counting internal retries. Metadata,
/// server-side copies, and HTTP framing do not contribute payload bytes.
#[derive(Clone, Debug)]
pub struct IoMetricsLayer(pub IoMetrics);
impl Layer for IoMetricsLayer {
    fn apply_service(&self, inner: Servicer) -> Servicer {
        Arc::new(Metered {
            inner,
            metrics: self.0.clone(),
        })
    }
}
#[derive(Debug)]
struct Metered<T> {
    inner: T,
    metrics: IoMetrics,
}
impl<T> Metered<T> {
    fn wrap<U>(&self, inner: U) -> Metered<U> {
        Metered {
            inner,
            metrics: self.metrics.clone(),
        }
    }
}
impl Service for Metered<Servicer> {
    type Reader = Metered<oio::Reader>;
    type Writer = Metered<oio::Writer>;
    type Lister = oio::Lister;
    type Deleter = oio::Deleter;
    type Copier = oio::Copier;
    type Composer = oio::Composer;
    fn info(&self) -> ServiceInfo {
        self.inner.info()
    }
    fn capability(&self) -> Capability {
        self.inner.capability()
    }
    fn read(&self, ctx: &OperationContext, path: &str, args: OpRead) -> Result<Self::Reader> {
        Ok(self.wrap(self.inner.read(ctx, path, args)?))
    }
    fn write(&self, ctx: &OperationContext, path: &str, args: OpWrite) -> Result<Self::Writer> {
        Ok(self.wrap(self.inner.write(ctx, path, args)?))
    }
    async fn create_dir(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpCreateDir,
    ) -> Result<RpCreateDir> {
        self.inner.create_dir(ctx, path, args).await
    }
    async fn stat(&self, ctx: &OperationContext, path: &str, args: OpStat) -> Result<RpStat> {
        self.inner.stat(ctx, path, args).await
    }
    fn list(&self, ctx: &OperationContext, path: &str, args: OpList) -> Result<Self::Lister> {
        self.inner.list(ctx, path, args)
    }
    async fn rename(
        &self,
        ctx: &OperationContext,
        from: &str,
        to: &str,
        args: OpRename,
    ) -> Result<RpRename> {
        self.inner.rename(ctx, from, to, args).await
    }
    async fn restore(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpRestore,
    ) -> Result<RpRestore> {
        self.inner.restore(ctx, path, args).await
    }
    async fn presign(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpPresign,
    ) -> Result<RpPresign> {
        self.inner.presign(ctx, path, args).await
    }
    fn delete(&self, ctx: &OperationContext) -> Result<Self::Deleter> {
        self.inner.delete(ctx)
    }
    fn copy(
        &self,
        ctx: &OperationContext,
        from: &str,
        to: &str,
        args: OpCopy,
    ) -> Result<Self::Copier> {
        self.inner.copy(ctx, from, to, args)
    }
    fn compose(&self, ctx: &OperationContext, to: &str, args: OpCompose) -> Result<Self::Composer> {
        self.inner.compose(ctx, to, args)
    }
}
impl oio::Read for Metered<oio::Reader> {
    async fn read(&self, range: BytesRange) -> Result<(RpRead, Buffer)> {
        let (reply, bytes) = self.inner.read(range).await?;
        self.metrics.record_read(bytes.len() as u64);
        Ok((reply, bytes))
    }
    async fn open(&self, range: BytesRange) -> Result<(RpRead, Box<dyn oio::ReadStreamDyn>)> {
        let (reply, inner) = self.inner.open(range).await?;
        Ok((reply, Box::new(self.wrap(inner))))
    }
}
impl oio::ReadStream for Metered<Box<dyn oio::ReadStreamDyn>> {
    async fn read(&mut self) -> Result<Buffer> {
        let bytes = self.inner.read().await?;
        self.metrics.record_read(bytes.len() as u64);
        Ok(bytes)
    }
}
impl oio::Write for Metered<oio::Writer> {
    async fn write(&mut self, bytes: Buffer) -> Result<()> {
        let size = bytes.len() as u64;
        self.inner.write(bytes).await?;
        self.metrics.record_write(size);
        Ok(())
    }
    async fn copy_from(&mut self, path: &str, args: OpRead, range: BytesRange) -> Result<()> {
        self.inner.copy_from(path, args, range).await
    }
    async fn close(&mut self) -> Result<Metadata> {
        self.inner.close().await
    }
    async fn abort(&mut self) -> Result<()> {
        self.inner.abort().await
    }
}
