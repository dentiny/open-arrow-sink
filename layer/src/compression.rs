use open_arrow_sink_core::{
    CompressionOptions, Layer, Result, SchemaRef, Sink, SinkCapability, WriteOptions, Writer,
    async_trait, validation,
};

/// Overrides the entire session compression policy. The backend performs encoding.
/// Like OrderingLayer, install one compression layer per sink.
#[derive(Debug, Clone)]
pub struct CompressionLayer {
    compression: CompressionOptions,
}

impl CompressionLayer {
    pub fn new(compression: CompressionOptions) -> Self {
        Self { compression }
    }
}

pub struct CompressionSink<S> {
    inner: S,
    compression: CompressionOptions,
}

impl<S: Sink> Layer<S> for CompressionLayer {
    type Layered = CompressionSink<S>;

    fn layer(&self, inner: S) -> Self::Layered {
        CompressionSink {
            inner,
            compression: self.compression.clone(),
        }
    }
}

#[async_trait]
impl<S: Sink> Sink for CompressionSink<S> {
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
        options.compression = self.compression.clone();
        validation::validate_options(&schema, &options, self.inner.capability())?;
        self.inner.open(request, schema, options).await
    }
}

#[async_trait]
impl<S: open_arrow_sink_core::ParallelSink> open_arrow_sink_core::ParallelSink
    for CompressionSink<S>
{
    type Output = S::Output;

    async fn open_writers(
        &self,
        request: Self::Request,
        schema: SchemaRef,
        mut options: WriteOptions,
        writers: std::num::NonZeroUsize,
    ) -> Result<open_arrow_sink_core::WriteSession<Self::Output>> {
        options.compression = self.compression.clone();
        validation::validate_options(&schema, &options, self.inner.capability())?;
        self.inner
            .open_writers(request, schema, options, writers)
            .await
    }
}
