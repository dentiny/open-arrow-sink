use crate::{Result, SchemaRef, SinkCapability, WriteOptions, Writer, async_trait};
use std::num::NonZeroUsize;

/// A sink that creates independent data writers within one publication session.
/// Reserve the destination once and return exactly `writers` schema-compatible
/// writers. Output names must be disjoint. Clean up a failed open according to
/// the cleanup policy; cancellation/drop never promises asynchronous cleanup.
#[async_trait]
pub trait ParallelSink: Sink {
    /// Backend-owned metadata, such as Iceberg data files or Lance fragments.
    type Output: Send + 'static;

    async fn open_writers(
        &self,
        request: Self::Request,
        schema: SchemaRef,
        options: WriteOptions,
        writers: NonZeroUsize,
    ) -> Result<crate::WriteSession<Self::Output>>;
}

/// Reusable configuration that opens independently owned write sessions.
#[async_trait]
pub trait Sink: Send + Sync {
    /// Backend-specific destination and options; no source-library types.
    type Request: Send;

    fn capability(&self) -> SinkCapability;

    /// Validate shared options and compression column names against the input schema
    /// with `validation::validate_options` before destination I/O.
    /// Sinks that support indexes also call `validation::validate_indexes` and validate
    /// backend-specific index types before destination I/O. Perform any other
    /// destination-specific validation before writing data. The input schema is
    /// fixed for the lifetime of the returned writer, including empty sessions.
    /// An ordering guarantee must never be silently downgraded.
    async fn open(
        &self,
        request: Self::Request,
        schema: SchemaRef,
        options: WriteOptions,
    ) -> Result<Box<dyn Writer>>;
}
