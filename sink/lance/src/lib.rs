//! Versioned Lance datasets over shared OpenDAL storage. Requires a Tokio runtime.

mod blob;
mod blob_input;
mod blob_writer;
mod committer;
mod compression;
mod error;
mod provider;
mod sink;
mod validation;
mod write_options;

pub use sink::LanceDatasetSink;

/// Uncommitted fragments produced by a Lance data writer.
///
/// Pass this opaque output to the same write session's committer to publish it.
/// Its fields are private so native Lance transaction types remain internal.
pub struct LanceFragments {
    rows_written: u64,
    transaction: lance::dataset::transaction::Transaction,
}

mod index;
mod io_metrics;

mod parallel;

mod storage;
mod storage_router;
pub use open_arrow_sink_storage::{Storage, Url};
