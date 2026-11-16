use open_arrow_sink_core::{Ordering, WriteOptions};

/// Explicitly request the guarantee all built-in backends currently provide.
pub fn unordered_options() -> WriteOptions {
    WriteOptions::default().with_ordering(Ordering::Unordered)
}
