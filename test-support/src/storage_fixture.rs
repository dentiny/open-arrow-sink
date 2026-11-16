/// An isolated in-memory operator rooted at the requested namespace.
pub fn memory_storage(base: &str) -> open_arrow_sink_storage::Storage {
    use open_arrow_sink_storage::{Storage, opendal};
    Storage::new(
        opendal::Operator::new(opendal::services::Memory::default()).unwrap(),
        base.parse().unwrap(),
    )
    .unwrap()
}
