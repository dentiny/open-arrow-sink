/// Guarantees provided by the configured sink.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SinkCapability {
    /// Preserve input row order in the target's documented read order.
    /// Sequential I/O completion alone does not provide this guarantee.
    pub preserve_input_order: bool,
}
