use arrow_schema::SchemaRef;
use open_arrow_sink_core::{
    Layer, Ordering, Result, Sink, SinkCapability, WriteOptions, Writer, async_trait, validation,
};

/// Override the shared ordering policy for each session opened through this layer.
/// This declares permission/requirements; backends implement the execution strategy.
#[derive(Debug, Clone, Copy)]
pub struct OrderingLayer {
    ordering: Ordering,
}

impl OrderingLayer {
    pub fn new(ordering: Ordering) -> Self {
        Self { ordering }
    }
}

pub struct OrderingSink<S> {
    inner: S,
    ordering: Ordering,
}

impl<S: Sink> Layer<S> for OrderingLayer {
    type Layered = OrderingSink<S>;

    fn layer(&self, inner: S) -> Self::Layered {
        OrderingSink {
            inner,
            ordering: self.ordering,
        }
    }
}

#[async_trait]
impl<S: Sink> Sink for OrderingSink<S> {
    type Request = S::Request;

    fn capability(&self) -> SinkCapability {
        // Permission to reorder does not create an ordering guarantee.
        self.inner.capability()
    }

    async fn open(
        &self,
        request: Self::Request,
        schema: SchemaRef,
        mut options: WriteOptions,
    ) -> Result<Box<dyn Writer>> {
        options.ordering = self.ordering;
        validation::validate_options(&schema, &options, self.inner.capability())?;
        self.inner.open(request, schema, options).await
    }
}
