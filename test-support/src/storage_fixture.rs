/// An isolated in-memory operator rooted at the requested namespace.
pub fn memory_storage(base: &str) -> open_arrow_sink_storage::Storage {
    use open_arrow_sink_storage::{Storage, opendal};
    Storage::new(
        opendal::Operator::new(opendal::services::Memory::default()).unwrap(),
        base.parse().unwrap(),
    )
    .unwrap()
}

/// Resolve a fixture-relative path, failing at the setup site on invalid input.
pub fn path(
    storage: &open_arrow_sink_storage::Storage,
    relative: &str,
) -> open_arrow_sink_storage::ObjectPath {
    storage
        .path(
            &storage
                .url(&open_arrow_sink_storage::ObjectPath::parse(relative).unwrap())
                .unwrap(),
        )
        .unwrap()
}

/// List a fixture directory in deterministic path order.
pub async fn sorted_list(
    storage: &open_arrow_sink_storage::Storage,
    directory: &open_arrow_sink_storage::ObjectPath,
) -> Vec<open_arrow_sink_storage::ObjectPath> {
    let mut paths = storage.list(directory).await.unwrap();
    paths.sort_by(|a, b| a.as_str().cmp(b.as_str()));
    paths
}
