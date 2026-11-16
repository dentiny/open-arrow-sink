//! Shared fixtures for workspace integration tests. Use as a dev-dependency only.
//! Backend-specific setup and deliberate malformed-HTTP fault injection stay in
//! their owning tests; the HTTP helper serves valid protocol responses.
pub mod http;

use arrow_array::{ArrayRef, Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use open_arrow_sink_core::{Ordering, WriteOptions};
use std::sync::Arc;

pub fn int64_schema(column: &str, nullable: bool) -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new(
        column,
        DataType::Int64,
        nullable,
    )]))
}

pub fn int64_batch(column: &str, values: Vec<Option<i64>>) -> RecordBatch {
    RecordBatch::try_from_iter([(column, Arc::new(Int64Array::from(values)) as ArrayRef)]).unwrap()
}

/// Explicitly request the guarantee all built-in backends currently provide.
pub fn unordered_options() -> WriteOptions {
    WriteOptions::default().with_ordering(Ordering::Unordered)
}
