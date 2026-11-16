use crate::Sink;

/// Wrap a sink without I/O, preserving its backend-specific request type.
/// Option decorators should leave unrelated option validation to the backend,
/// after inner layers have applied their overrides.
pub trait Layer<S: Sink> {
    /// Decorated sink, retaining the inner sink's backend-specific request type.
    type Layered: Sink<Request = S::Request>;

    /// Wrap `inner` with this layer's behavior without performing destination I/O.
    /// The returned sink applies the layer when sessions are opened or used.
    fn layer(&self, inner: S) -> Self::Layered;
}

/// Fluent layer composition, available to every sized [`Sink`].
pub trait SinkExt: Sink + Sized {
    /// The last installed layer is outermost: `sink.layer(a).layer(b)` sends
    /// open/write operations through b, then a, then the sink.
    fn layer<L: Layer<Self>>(self, layer: L) -> L::Layered {
        layer.layer(self)
    }
}

impl<S: Sink> SinkExt for S {}
