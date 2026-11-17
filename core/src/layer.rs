use std::future::Future;

use futures::Stream;

use crate::{RecordBatch, SchemaRef, Sink, WriteAllError, WriteOptions, WriteSummary};

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

/// Fluent layer composition and stream writing, available to every sized [`Sink`].
pub trait SinkExt: Sink + Sized {
    /// Write an entire fallible batch stream, then commit once at EOF.
    ///
    /// Input and write errors trigger a best-effort abort; abort errors never
    /// replace the original error. Open errors return before polling input.
    /// Commit consumes the writer: commit errors return directly, without abort
    /// or retry, because publication may already have succeeded.
    ///
    /// Input is pinned internally and pulled one batch at a time, with no
    /// collection or extra buffering. Empty input still opens and commits using
    /// the supplied schema. Cancelling this future or panicking drops the writer
    /// without asynchronous abort, following the normal writer contract.
    fn write_all<S, E>(
        &self,
        request: Self::Request,
        schema: SchemaRef,
        options: WriteOptions,
        batches: S,
    ) -> impl Future<Output = Result<WriteSummary, WriteAllError<E>>> + Send
    where
        S: Stream<Item = Result<RecordBatch, E>> + Send,
        E: Send,
    {
        self.write_all_with_progress(request, schema, options, batches, |_| {})
    }

    /// Like [`Self::write_all`], invoking `progress(batch_rows)` once after each
    /// successful `Writer::write`, including zero-row batches. This reports
    /// accepted input, not committed output; a later write or commit can fail,
    /// and backend policies may filter or transform accepted rows.
    /// The callback is synchronous and infallible; a panic does not run abort.
    fn write_all_with_progress<S, E, F>(
        &self,
        request: Self::Request,
        schema: SchemaRef,
        options: WriteOptions,
        batches: S,
        progress: F,
    ) -> impl Future<Output = Result<WriteSummary, WriteAllError<E>>> + Send
    where
        S: Stream<Item = Result<RecordBatch, E>> + Send,
        E: Send,
        F: FnMut(usize) + Send,
    {
        crate::write_all::write_all(self, request, schema, options, batches, progress)
    }

    /// The last installed layer is outermost: `sink.layer(a).layer(b)` sends
    /// open/write operations through b, then a, then the sink.
    fn layer<L: Layer<Self>>(self, layer: L) -> L::Layered {
        layer.layer(self)
    }
}

impl<S: Sink> SinkExt for S {}
