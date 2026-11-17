use crate::{Result, SchemaRef, SinkCapability, WriteOptions, Writer, async_trait};
use std::num::NonZeroUsize;

/// A sink that creates independent data writers within one publication session.
/// Reserve the destination once and return exactly `writers` schema-compatible
/// writers. Output names must be disjoint. Failed opens leave output in place;
/// cancellation/drop never performs asynchronous cleanup.
#[async_trait]
pub trait ParallelSink: Sink {
    /// Backend-owned metadata, such as Iceberg data files or Lance fragments.
    type Output: Send + 'static;

    /// Open one data writer using the parallel session lifecycle.
    /// `Sink::open` can delegate here unless it needs a specialized writer.
    /// Invalid writer counts or schemas are rejected without invoking abort.
    async fn open_writer(
        &self,
        request: Self::Request,
        schema: SchemaRef,
        options: WriteOptions,
    ) -> Result<Box<dyn Writer>> {
        self.open_writers(request, schema.clone(), options, NonZeroUsize::MIN)
            .await?
            .expect(1, &schema)?
            .into_writer()
    }

    /// Validate effective options and the schema before destination I/O, just as
    /// `Sink::open` does. Layers may override options before delegating here.
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

    /// Return the operations and guarantees supported by this configured sink.
    /// Layers report the capabilities of the decorated sink. Opening a session
    /// still validates the requested options and schema against these capabilities.
    fn capability(&self) -> SinkCapability;

    /// Check schema and options without destination I/O. The default checks shared
    /// options; backends override this to add their schema and type constraints.
    /// A successful preflight cannot predict destination state or data-dependent
    /// failures (for example, index training or malformed blob URLs in rows).
    fn check_schema(
        &self,
        schema: &arrow_schema::Schema,
        options: &WriteOptions,
    ) -> Vec<crate::SchemaIssue> {
        crate::validation::check_options(schema, options, self.capability())
    }

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
