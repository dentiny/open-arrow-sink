use crate::{Result, SchemaRef, SinkCapability, WriteOptions, Writer, async_trait};
use std::num::NonZeroUsize;

/// A backend that can reserve multiple independent writers for one destination.
/// Success returns exactly the requested number of writers with the supplied
/// schema. Writers must use disjoint output names and honor the options. On an
/// open error, clean up already opened writers according to cleanup_on_error.
/// Commits are independent: a later failure cannot roll back earlier commits.
/// Abort must only remove output owned by that writer, preserving other writers.
/// After cancellation of open, drop never commits and async cleanup is not promised.
#[async_trait]
pub trait ParallelSink: Sink {
    async fn open_writers(
        &self,
        request: Self::Request,
        schema: SchemaRef,
        options: WriteOptions,
        writers: NonZeroUsize,
    ) -> Result<Vec<Box<dyn Writer>>>;
}

/// Reusable configuration that opens independently owned write sessions.
#[async_trait]
pub trait Sink: Send + Sync {
    /// Backend-specific destination and options; no source-library types.
    type Request: Send;

    fn capability(&self) -> SinkCapability;

    /// Validate shared options with `validation::validate_options`, then perform
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
