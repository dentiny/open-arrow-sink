//! Shared fixtures for workspace integration tests. Use as a dev-dependency only.
//! Backend-specific setup and deliberate malformed-HTTP fault injection stay in
//! their owning tests; the HTTP helper serves valid protocol responses.
mod batch;
pub mod conformance;
pub mod http;
mod options;
mod payload;

pub use batch::{concat_sorted, int64_batch, int64_schema, sort_batch};
pub use options::unordered_options;
pub use payload::seeded_bytes;

mod parquet_fixture;
mod storage_fixture;
pub use parquet_fixture::read_parquet_dir;
pub use storage_fixture::{memory_storage, path, sorted_list};

/// Bound asynchronous fixture work so a leaked permit fails instead of hanging.
pub async fn within<F: std::future::Future>(future: F) -> F::Output {
    tokio::time::timeout(std::time::Duration::from_secs(5), future)
        .await
        .expect("fixture timed out")
}
