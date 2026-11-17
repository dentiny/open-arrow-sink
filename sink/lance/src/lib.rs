//! Versioned Lance datasets over shared OpenDAL storage. Requires a Tokio runtime.

mod blob;
mod blob_input;
mod committer;
mod compression;
mod error;
mod provider;
mod schema_check;
mod sink;
mod validation;
mod worker;
mod write_options;

pub use sink::LanceDatasetSink;

/// Uncommitted fragments produced by a Lance data writer.
///
/// Pass this opaque output to the same write session's committer to publish it.
/// Its fields are private so native Lance transaction types remain internal.
pub struct LanceFragments {
    summary: open_arrow_sink_core::CommitSummary,
    transaction: lance::dataset::transaction::Transaction,
}

mod index;
mod indexed_commit;

mod parallel;

mod destination;
pub use open_arrow_sink_storage::{Storage, Url};
