//! Shared fixtures for workspace integration tests. Use as a dev-dependency only.
//! Backend-specific setup and deliberate malformed-HTTP fault injection stay in
//! their owning tests; the HTTP helper serves valid protocol responses.
mod batch;
pub mod http;
mod options;
mod payload;

pub use batch::{concat_sorted, int64_batch, int64_schema, sort_batch};
pub use options::unordered_options;
pub use payload::seeded_bytes;
