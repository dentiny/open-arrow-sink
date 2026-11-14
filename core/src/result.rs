/// Number of rows successfully committed by this session.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct WriteSummary {
    pub rows_written: u64,
}
