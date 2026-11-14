use crate::{Result, SchemaRef, SinkCapability, WriteOptions, Writer, async_trait};

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
