use std::time::Duration;

use open_arrow_sink_storage::{RetryConfig, Storage, StorageConfig, TimeoutConfig, Url, opendal};

fn memory() -> Storage {
    Storage::new(
        opendal::Operator::new(opendal::services::Memory::default()).unwrap(),
        "memory:///warehouse/".parse().unwrap(),
    )
    .unwrap()
}

#[tokio::test]
async fn streaming_close_abort_and_shared_namespace() {
    let storage = memory();
    let path = storage
        .resolve(&storage.url("雪 space/file").unwrap())
        .unwrap();
    let root = storage.resolve(storage.base_url()).unwrap();
    let mut writer = storage.writer(&path).await.unwrap();
    writer.write("hello ".into()).await.unwrap();
    writer.write("world".into()).await.unwrap();
    assert_eq!(writer.close().await.unwrap(), 11);
    assert!(writer.write("late".into()).await.is_err());
    assert_eq!(storage.clone().read(&path).await.unwrap(), "hello world");
    assert_eq!(storage.read_range(&path, 6..11).await.unwrap(), "world");
    let metadata = storage.stat_optional(&path).await.unwrap().unwrap();
    assert!(metadata.is_file());
    assert_eq!(metadata.content_length(), 11);
    assert_eq!(storage.list(&root).await.unwrap(), vec![path.clone()]);
    assert!(storage.create(&path, "replace".into()).await.is_err());
    assert_eq!(storage.read(&path).await.unwrap(), "hello world");
    let aborted = root.join("aborted").unwrap();
    let mut writer = storage.writer(&aborted).await.unwrap();
    writer.write("incomplete".into()).await.unwrap();
    writer.abort().await.unwrap();
    assert!(!storage.exists(&aborted).await.unwrap());
    storage.delete(&path).await.unwrap();
    assert!(storage.stat_optional(&path).await.unwrap().is_none());
    storage.delete(&path).await.unwrap();
    assert!(storage.list(&root).await.unwrap().is_empty());
}

#[tokio::test]
async fn namespace_boundaries_and_conditional_creation() {
    let storage = memory();
    for url in [
        "memory:///warehouse2/file",
        "memory:///outside/file",
        "s3://bucket/warehouse/file",
        "memory:///warehouse/file?x=y",
        "memory:///warehouse/file#fragment",
        "memory:///warehouse/%2e%2e%2ffile",
    ] {
        assert!(storage.resolve(&url.parse().unwrap()).is_err(), "{url}");
    }
    assert!(storage.url("../outside").is_err());
    let root = storage.resolve(storage.base_url()).unwrap();
    assert!(root.is_root());
    assert!(root.as_dir().is_root());
    let repeated_root = storage
        .resolve(&"memory:///warehouse//".parse().unwrap())
        .unwrap();
    assert!(repeated_root.is_root());
    assert!(storage.remove_all(&root).await.is_err());
    assert!(storage.remove_all(&repeated_root).await.is_err());
    assert!(storage.prefix_exists(&root).await.is_err());
    let prefix = root.join("claim").unwrap();
    assert_eq!(prefix.as_dir().as_str(), "claim/");
    assert!(!storage.prefix_exists(&prefix).await.unwrap());
    let (a, b) = tokio::join!(
        storage.create(&prefix, "a".into()),
        storage.create(&prefix, "b".into())
    );
    assert_ne!(a.is_ok(), b.is_ok());
    assert!(storage.prefix_exists(&prefix).await.unwrap());
}

#[tokio::test]
async fn filesystem_urls_decode_and_preserve_siblings() {
    let temp = tempfile::tempdir().unwrap();
    let root = Url::from_directory_path(temp.path()).unwrap();
    let storage = Storage::from_url(&root).unwrap();
    let output = root.join("new/").unwrap();
    let file = output.join("part%20%E9%9B%AA").unwrap();
    storage
        .create(&storage.resolve(&file).unwrap(), "data".into())
        .await
        .unwrap();
    assert_eq!(
        storage
            .read(&storage.resolve(&file).unwrap())
            .await
            .unwrap(),
        "data"
    );
    assert!(
        storage
            .stat_optional(&storage.resolve(&output).unwrap())
            .await
            .unwrap()
            .unwrap()
            .is_dir()
    );
    let aborted = root.join("aborted").unwrap();
    let mut writer = storage
        .writer(&storage.resolve(&aborted).unwrap())
        .await
        .unwrap();
    writer.write("incomplete".into()).await.unwrap();
    writer.abort().await.unwrap();
    // Non-staged files can be visible before close; the owner removes them.
    storage
        .delete(&storage.resolve(&aborted).unwrap())
        .await
        .unwrap();
    let sibling = root.join("keep").unwrap();
    storage
        .create(&storage.resolve(&sibling).unwrap(), "keep".into())
        .await
        .unwrap();
    storage
        .remove_all(&storage.resolve(&output).unwrap())
        .await
        .unwrap();
    assert!(
        !storage
            .exists(&storage.resolve(&file).unwrap())
            .await
            .unwrap()
    );
    assert_eq!(
        storage
            .read(&storage.resolve(&sibling).unwrap())
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
    let path = storage.resolve(&url).unwrap();
    let (first, second) = tokio::join!(
        storage.create(&path, "first".into()),
        storage.create(&path, "second".into())
    );
    assert_ne!(first.is_ok(), second.is_ok());
    assert_eq!(
        storage.read(&storage.resolve(&url).unwrap()).await.unwrap(),
        if first.is_ok() { "first" } else { "second" }
    );
}

#[tokio::test]
async fn errors_retain_operation_path_and_opendal_source() {
    use std::error::Error as _;
    let storage = memory();
    let error = storage
        .read(&storage.resolve(&storage.url("missing").unwrap()).unwrap())
        .await
        .unwrap_err();
    assert!(error.to_string().contains("read object \"missing\""));
    let mut source = error.source();
    while let Some(cause) = source {
        if let Some(error) = cause.downcast_ref::<opendal::Error>() {
            assert_eq!(error.kind(), opendal::ErrorKind::NotFound);
            return;
        }
        source = cause.source();
    }
    panic!("the original OpenDAL error must remain in the source chain");
}

#[test]
fn resolved_paths_preserve_decoded_keys_and_reject_traversal() {
    let storage = memory();
    let url = storage.url("雪 space/literal%20").unwrap();
    let path = storage.resolve(&url).unwrap();
    assert_eq!(path.as_str(), "雪 space/literal%20");
    assert_eq!(storage.url(path.as_str()).unwrap(), url);
    assert_eq!(path.as_dir().as_dir(), path.as_dir());
    assert_eq!(path.as_dir().as_object(), path);
    for child in [
        "../outside",
        "nested/../outside",
        "/absolute",
        "bad\\path",
        "bad\0path",
    ] {
        assert!(path.join(child).is_err(), "{child:?}");
    }
    let child = path.as_dir().join("literal%2f雪").unwrap();
    assert_eq!(child.as_str(), "雪 space/literal%20/literal%2f雪");
}
