use open_arrow_sink_core::{
    Layer, ParallelSink, Result, SchemaRef, Sink, SinkCapability, WriteOptions, WriteSession,
    Writer, async_trait,
};
use std::{num::NonZeroUsize, sync::Arc};

type Policy = dyn Fn(&mut WriteOptions, SinkCapability) -> Result<()> + Send + Sync;

/// Overrides session options before the backend validates and opens the destination.
/// The same policy applies to both single-writer and parallel sessions.
#[derive(Clone)]
pub struct OptionsLayer {
    policy: Arc<Policy>,
}

impl OptionsLayer {
    pub fn new(update: impl Fn(&mut WriteOptions) + Send + Sync + 'static) -> Self {
        Self::checked(move |options, _| {
            update(options);
            Ok(())
        })
    }

    pub(crate) fn checked(
        policy: impl Fn(&mut WriteOptions, SinkCapability) -> Result<()> + Send + Sync + 'static,
    ) -> Self {
        Self {
            policy: Arc::new(policy),
        }
    }
}

pub struct OptionsSink<S> {
    inner: S,
    policy: Arc<Policy>,
}

impl<S: Sink> Layer<S> for OptionsLayer {
    type Layered = OptionsSink<S>;

    fn layer(&self, inner: S) -> Self::Layered {
        OptionsSink {
            inner,
            policy: self.policy.clone(),
        }
    }
}

#[async_trait]
impl<S: Sink> Sink for OptionsSink<S> {
    type Request = S::Request;

    fn capability(&self) -> SinkCapability {
        self.inner.capability()
    }

    async fn open(
        &self,
        request: Self::Request,
        schema: SchemaRef,
        mut options: WriteOptions,
    ) -> Result<Box<dyn Writer>> {
        (self.policy)(&mut options, self.inner.capability())?;
        self.inner.open(request, schema, options).await
    }
}

#[async_trait]
impl<S: ParallelSink> ParallelSink for OptionsSink<S> {
    type Output = S::Output;

    async fn open_writers(
        &self,
        request: Self::Request,
        schema: SchemaRef,
        mut options: WriteOptions,
        writers: NonZeroUsize,
    ) -> Result<WriteSession<Self::Output>> {
        (self.policy)(&mut options, self.inner.capability())?;
        self.inner
            .open_writers(request, schema, options, writers)
            .await
    }
}

#[async_trait]
impl<S: open_arrow_sink_blob::BlobSink> open_arrow_sink_blob::BlobSink for OptionsSink<S> {
    async fn open_blobs(
        &self,
        request: Self::Request,
        schema: SchemaRef,
        mut options: WriteOptions,
    ) -> Result<Box<dyn open_arrow_sink_blob::BlobSessionWriter>> {
        (self.policy)(&mut options, self.inner.capability())?;
        self.inner.open_blobs(request, schema, options).await
    }
}
