/// Number of rows successfully committed by this session.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct WriteSummary {
    pub rows_written: u64,
    /// Successful backend payload reads during the session; None when unmeasured.
    pub bytes_read: Option<u64>,
    /// Successful backend payload writes during the session; None when unmeasured.
    pub bytes_written: Option<u64>,
}
