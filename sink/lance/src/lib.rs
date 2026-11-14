//! Create-only local Lance datasets. Requires a Tokio runtime.

mod error;
mod sink;
mod writer;

pub use sink::LanceDatasetSink;

mod index;
mod partition;
pub use index::{
    IndexBuildReport, IndexBuildResult, IndexBuildState, IndexCommitError, IndexedLanceSink,
    LanceIndexKind, LanceIndexLayer, LanceIndexSpec, retry_indexes,
};
pub use partition::{
    LancePartitionLayer, PartitionCommitError, PartitionCommitResult, PartitionCommitState,
    PartitionKey, PartitionedLanceSink,
};

mod private {
    pub trait Sealed {}
}

/// Dataset sinks supported by the Lance partition layer. Sealed to prevent
/// accidentally routing non-Lance sinks or nesting partition layers.
pub trait LanceSink:
    open_arrow_sink_core::Sink<Request = std::path::PathBuf> + Clone + private::Sealed + 'static
{
    /// Validate backend configuration before reserving any partition directory.
    fn validate_schema(&self, schema: &arrow_schema::Schema) -> open_arrow_sink_core::Result<()>;
}
impl private::Sealed for LanceDatasetSink {}
impl LanceSink for LanceDatasetSink {
    fn validate_schema(&self, _schema: &arrow_schema::Schema) -> open_arrow_sink_core::Result<()> {
        Ok(())
    }
}

fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write;
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        write!(&mut encoded, "{byte:02x}").unwrap();
    }
    encoded
}
