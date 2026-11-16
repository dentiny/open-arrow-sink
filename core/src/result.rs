/// Outcome of a successful write session.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct WriteSummary {
    pub rows_written: u64,
}
