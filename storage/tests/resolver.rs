use open_arrow_sink_storage::{ObjectPath, Storage, StorageResolver, Url, opendal};

fn memory(base: &str) -> Storage {
    open_arrow_sink_test_support::memory_storage(base)
}

async fn read_from(storage: Storage, url: &Url) -> bytes::Bytes {
    let path = storage.path(url).unwrap();
    storage.read(&path).await.unwrap()
}

#[tokio::test]
async fn specific_roots_win_regardless_of_registration_order() {
    let broad = memory("s3://source/");
    let specific = memory("s3://source/private/");
    let other = memory("s3://other/private/");
    let url = specific.url(&ObjectPath::parse("key").unwrap()).unwrap();
    let other_url = other.url(&ObjectPath::parse("key").unwrap()).unwrap();
    broad
        .write(&broad.path(&url).unwrap(), "broad".into())
        .await
        .unwrap();
    specific
        .write(&specific.path(&url).unwrap(), "specific".into())
        .await
        .unwrap();
    other
        .write(&other.path(&other_url).unwrap(), "other".into())
        .await
        .unwrap();
    for stores in [
        [broad.clone(), specific.clone()],
        [specific.clone(), broad.clone()],
    ] {
        let resolver = StorageResolver::default()
            .with_storage(stores[0].clone())
            .with_storage(stores[1].clone())
            .with_storage(other.clone());
        assert_eq!(
            read_from(resolver.storage_for(&url).unwrap(), &url).await,
            "specific"
        );
        assert_eq!(
            read_from(resolver.storage_for(&other_url).unwrap(), &other_url).await,
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
        read_from(original.storage_for(&url).unwrap(), &url).await,
        "filesystem"
    );

    // The clone shares an already populated file cache, but its explicit operator
    // must take precedence. Registrations on the clone must not alter the original.
    let explicit = memory(Url::from_directory_path(temp.path()).unwrap().as_str());
    explicit
        .write(&explicit.path(&url).unwrap(), "configured".into())
        .await
        .unwrap();
    let configured = original.clone().with_storage(explicit);
    assert_eq!(
        read_from(configured.storage_for(&url).unwrap(), &url).await,
        "configured"
    );
    assert_eq!(
        read_from(original.storage_for(&url).unwrap(), &url).await,
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
    let source = resolver.storage_for(&url).unwrap();
    source
        .write(&source.path(&url).unwrap(), "custom backend".into())
        .await
        .unwrap();
    assert_eq!(
        read_from(resolver.storage_for(&url).unwrap(), &url).await,
        "custom backend"
    );
}
