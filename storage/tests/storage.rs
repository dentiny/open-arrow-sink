use std::time::Duration;

use open_arrow_sink_core::Error;

use open_arrow_sink_storage::{
    DirectoryState, ObjectPath, RetryConfig, Storage, StorageConfig, TimeoutConfig, Url, opendal,
};

fn memory() -> Storage {
    open_arrow_sink_test_support::memory_storage("memory:///warehouse/")
}

#[tokio::test]
async fn streaming_close_publishes_chunks_in_the_shared_namespace() {
    let storage = memory();
    let path = ObjectPath::parse("雪 space/file").unwrap();
    let mut writer = storage.writer(&path).await.unwrap();
    writer.write("hello ".into()).await.unwrap();
    writer.write("world".into()).await.unwrap();
    assert_eq!(writer.close().await.unwrap(), 11);
    assert!(writer.write("late".into()).await.is_err());
    assert_eq!(storage.clone().read(&path).await.unwrap(), "hello world");
}

#[tokio::test]
async fn reads_ranges_and_lists_object_metadata() {
    let storage = memory();
    let path = ObjectPath::parse("雪 space/file").unwrap();
    storage.write(&path, "hello world".into()).await.unwrap();
    assert_eq!(storage.read_range(&path, 6..11).await.unwrap(), "world");
    let metadata = storage.stat(&path).await.unwrap().unwrap();
    assert!(metadata.is_file());
    assert_eq!(metadata.content_length(), 11);
    assert_eq!(
        storage
            .list(&storage.path(storage.base_url()).unwrap())
            .await
            .unwrap(),
        vec![path]
    );
}

#[tokio::test]
async fn create_collision_preserves_existing_content() {
    let storage = memory();
    let path = ObjectPath::parse("file").unwrap();
    storage.create(&path, "original".into()).await.unwrap();
    assert!(storage.create(&path, "replace".into()).await.is_err());
    assert_eq!(storage.read(&path).await.unwrap(), "original");
}

#[tokio::test]
async fn abort_discards_incomplete_upload() {
    let storage = memory();
    let path = ObjectPath::parse("aborted").unwrap();
    let mut writer = storage.writer(&path).await.unwrap();
    writer.write("incomplete".into()).await.unwrap();
    writer.abort().await.unwrap();
    assert!(storage.stat(&path).await.unwrap().is_none());
}

#[tokio::test]
async fn delete_is_idempotent_and_removes_the_listing() {
    let storage = memory();
    let path = ObjectPath::parse("file").unwrap();
    storage.write(&path, "data".into()).await.unwrap();
    storage.delete(&path).await.unwrap();
    assert!(storage.stat(&path).await.unwrap().is_none());
    storage.delete(&path).await.unwrap();
    assert!(
        storage
            .list(&storage.path(storage.base_url()).unwrap())
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn namespace_boundaries_and_conditional_creation() {
    let storage = memory();
    for url in [
        "memory:///warehouse2/file",
        "memory:///outside/file",
        "s3://bucket/warehouse/file",
        "memory:///warehouse/file?x=y",
        "memory:///warehouse/%2e%2e%2ffile",
    ] {
        assert!(
            matches!(
                storage.path(&url.parse().unwrap()),
                Err(Error::InvalidInput(_))
            ),
            "{url}"
        );
    }
    assert!(matches!(
        ObjectPath::parse("../outside"),
        Err(Error::InvalidInput(_))
    ));
    let root = storage.path(storage.base_url()).unwrap();
    let repeated_root = storage
        .path(&"memory:///warehouse//".parse().unwrap())
        .unwrap();
    assert!(storage.remove_all(&root).await.is_err());
    assert!(storage.remove_all(&repeated_root).await.is_err());
    assert!(storage.inspect_directory(&root).await.is_err());
    let prefix = root.join("claim").unwrap();
    assert_eq!(prefix.as_dir().as_str(), "claim/");
    assert_eq!(
        storage.inspect_directory(&prefix).await.unwrap(),
        DirectoryState::Missing
    );
    let (a, b) = tokio::join!(
        storage.create(&prefix, "a".into()),
        storage.create(&prefix, "b".into())
    );
    assert_ne!(a.is_ok(), b.is_ok());
    assert_eq!(
        storage.inspect_directory(&prefix).await.unwrap(),
        DirectoryState::File
    );
}

#[tokio::test]
async fn filesystem_urls_decode_and_preserve_siblings() {
    let temp = tempfile::tempdir().unwrap();
    let root = Url::from_directory_path(temp.path()).unwrap();
    let storage = Storage::from_url(&root).unwrap();
    let output = root.join("new/").unwrap();
    let file = output.join("part%20%E9%9B%AA").unwrap();
    storage
        .create(&storage.path(&file).unwrap(), "data".into())
        .await
        .unwrap();
    assert_eq!(
        storage.read(&storage.path(&file).unwrap()).await.unwrap(),
        "data"
    );
    assert!(
        storage
            .stat(&storage.path(&output).unwrap())
            .await
            .unwrap()
            .unwrap()
            .is_dir()
    );
    let aborted = root.join("aborted").unwrap();
    let mut writer = storage
        .writer(&storage.path(&aborted).unwrap())
        .await
        .unwrap();
    writer.write("incomplete".into()).await.unwrap();
    writer.abort().await.unwrap();
    // Non-staged files can be visible before close; the owner removes them.
    storage
        .delete(&storage.path(&aborted).unwrap())
        .await
        .unwrap();
    let sibling = root.join("keep").unwrap();
    storage
        .create(&storage.path(&sibling).unwrap(), "keep".into())
        .await
        .unwrap();
    storage
        .remove_all(&storage.path(&output).unwrap())
        .await
        .unwrap();
    assert!(
        storage
            .stat(&storage.path(&file).unwrap())
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        storage
            .read(&storage.path(&sibling).unwrap())
            .await
            .unwrap(),
        "keep"
    );
}

#[test]
fn storage_policies_validate_without_io() {
    let operator = opendal::Operator::new(opendal::services::Memory::default()).unwrap();
    StorageConfig::default().apply(operator.clone()).unwrap();
    let config = StorageConfig {
        retry: Some(RetryConfig {
            factor: f32::NAN,
            ..Default::default()
        }),
        timeout: None,
    };
    assert!(matches!(
        config.apply(operator.clone()),
        Err(open_arrow_sink_core::Error::InvalidConfig(_))
    ));
    let config = StorageConfig {
        retry: None,
        timeout: Some(TimeoutConfig {
            io_timeout: Duration::ZERO,
            ..Default::default()
        }),
    };
    assert!(matches!(
        config.apply(operator),
        Err(open_arrow_sink_core::Error::InvalidConfig(_))
    ));
    assert!(
        StorageConfig {
            retry: Some(RetryConfig {
                min_delay: Duration::from_secs(2),
                max_delay: Duration::from_secs(1),
                ..Default::default()
            }),
            ..Default::default()
        }
        .validate()
        .is_err()
    );
}

#[tokio::test]
async fn filesystem_conditional_creation_has_one_winner() {
    let temp = tempfile::tempdir().unwrap();
    let url = Url::from_file_path(temp.path().join("claim")).unwrap();
    let storage = Storage::from_url(&url).unwrap();
    let path = storage.path(&url).unwrap();
    let (first, second) = tokio::join!(
        storage.create(&path, "first".into()),
        storage.create(&path, "second".into())
    );
    assert_ne!(first.is_ok(), second.is_ok());
    assert_eq!(
        storage.read(&storage.path(&url).unwrap()).await.unwrap(),
        if first.is_ok() { "first" } else { "second" }
    );
}

#[tokio::test]
async fn errors_retain_category_operation_and_path() {
    let storage = memory();
    let error = storage
        .read(&ObjectPath::parse("missing").unwrap())
        .await
        .unwrap_err();
    assert!(matches!(error, Error::NotFound(_)));
    assert!(error.to_string().contains("read object \"missing\""));
}

#[test]
fn object_paths_round_trip_through_urls_without_double_decoding() {
    let storage = memory();
    for key in ["", "file", "雪 space/file", "literal%20/?#", "directory/"] {
        let path = ObjectPath::parse(key).unwrap();
        let url = storage.url(&path).unwrap();
        assert_eq!(storage.path(&url).unwrap(), path, "{key}");
        assert!(url.query().is_none());
        assert!(url.fragment().is_none());
    }
    for key in ["/absolute", "../outside", "a/../b", "a/./b", "a\\b", "a\0b"] {
        assert!(
            matches!(ObjectPath::parse(key), Err(Error::InvalidInput(_))),
            "{key}"
        );
    }
}
