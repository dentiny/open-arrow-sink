use open_arrow_sink_storage::{Storage, StorageResolver, Url, opendal};

fn memory(base: &str) -> Storage {
    open_arrow_sink_test_support::memory_storage(base)
}

#[tokio::test]
async fn specific_roots_win_regardless_of_registration_order() {
    let broad = memory("s3://source/");
    let specific = memory("s3://source/private/");
    let other = memory("s3://other/private/");
    let url = specific.url("key").unwrap();
    let other_url = other.url("key").unwrap();
    broad.write(&url, "broad".into()).await.unwrap();
    specific.write(&url, "specific".into()).await.unwrap();
    other.write(&other_url, "other".into()).await.unwrap();
    for stores in [
        [broad.clone(), specific.clone()],
        [specific.clone(), broad.clone()],
    ] {
        let resolver = StorageResolver::default()
            .with_storage(stores[0].clone())
            .with_storage(stores[1].clone())
            .with_storage(other.clone());
        assert_eq!(
            resolver.resolve(&url).unwrap().read(&url).await.unwrap(),
            "specific"
        );
        assert_eq!(
            resolver
                .resolve(&other_url)
                .unwrap()
                .read(&other_url)
                .await
                .unwrap(),
            "other"
        );
    }
}

#[tokio::test]
async fn explicit_configuration_overrides_url_cache_without_changing_other_clones() {
    let temp = tempfile::tempdir().unwrap();
    let url = Url::from_file_path(temp.path().join("key")).unwrap();
    tokio::fs::write(temp.path().join("key"), "filesystem")
        .await
        .unwrap();
    let original = StorageResolver::default();
    assert_eq!(
        original.resolve(&url).unwrap().read(&url).await.unwrap(),
        "filesystem"
    );

    // The clone shares an already populated file cache, but its explicit operator
    // must take precedence. Registrations on the clone must not alter the original.
    let explicit = memory(Url::from_directory_path(temp.path()).unwrap().as_str());
    explicit.write(&url, "configured".into()).await.unwrap();
    let configured = original.clone().with_storage(explicit);
    assert_eq!(
        configured.resolve(&url).unwrap().read(&url).await.unwrap(),
        "configured"
    );
    assert_eq!(
        original.resolve(&url).unwrap().read(&url).await.unwrap(),
        "filesystem"
    );
}

#[tokio::test]
async fn discovers_services_registered_through_opendal() {
    let scheme = "resolver-custom";
    opendal::OperatorRegistry::get().register::<opendal::services::Memory>(scheme);
    let resolver = StorageResolver::default().with_storage(memory("explicit-only://source/"));
    let schemes: std::collections::HashSet<_> = resolver.schemes().collect();
    assert!(schemes.contains(scheme));
    assert!(schemes.contains("file"));
    assert!(schemes.contains("explicit-only"));

    let url = Url::parse("resolver-custom://source/key").unwrap();
    let source = resolver.resolve(&url).unwrap();
    source.write(&url, "custom backend".into()).await.unwrap();
    assert_eq!(
        resolver.resolve(&url).unwrap().read(&url).await.unwrap(),
        "custom backend"
    );
}
