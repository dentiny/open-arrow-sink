use std::time::Duration;

use open_arrow_sink_core::Error;

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
    let url = storage.url("雪 space/file").unwrap();
    let mut writer = storage.writer(&url).await.unwrap();
    writer.write("hello ".into()).await.unwrap();
    writer.write("world".into()).await.unwrap();
    assert_eq!(writer.close().await.unwrap(), 11);
    assert!(writer.write("late".into()).await.is_err());
    assert_eq!(storage.clone().read(&url).await.unwrap(), "hello world");
    assert_eq!(storage.read_range(&url, 6..11).await.unwrap(), "world");
    let metadata = storage.stat_optional(&url).await.unwrap().unwrap();
    assert!(metadata.is_file());
    assert_eq!(metadata.content_length(), 11);
    assert_eq!(
        storage.list(storage.base_url()).await.unwrap(),
        vec![url.clone()]
    );
    assert!(storage.create(&url, "replace".into()).await.is_err());
    assert_eq!(storage.read(&url).await.unwrap(), "hello world");
    let aborted = storage.url("aborted").unwrap();
    let mut writer = storage.writer(&aborted).await.unwrap();
    writer.write("incomplete".into()).await.unwrap();
    writer.abort().await.unwrap();
    assert!(!storage.exists(&aborted).await.unwrap());
    storage.delete(&url).await.unwrap();
    assert!(storage.stat_optional(&url).await.unwrap().is_none());
    storage.delete(&url).await.unwrap();
    assert!(storage.list(storage.base_url()).await.unwrap().is_empty());
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
        assert!(matches!(
            storage.stat_optional(&url.parse().unwrap()).await,
            Err(Error::InvalidInput(_))
        ));
    }
    assert!(
        storage
            .read_range(&"memory:///outside/file".parse().unwrap(), 0..0)
            .await
            .is_err()
    );
    assert!(matches!(
        storage.url("../outside"),
        Err(Error::InvalidInput(_))
    ));
    assert!(storage.remove_all(storage.base_url()).await.is_err());
    assert!(storage.directory_url(storage.base_url()).is_err());
    assert!(
        storage
            .directory_url(&"memory:///warehouse//".parse().unwrap())
            .is_err()
    );
    let prefix = storage.url("claim").unwrap();
    assert_eq!(
        storage.directory_url(&prefix).unwrap().path(),
        "/warehouse/claim/"
    );
    assert!(!storage.prefix_exists(&prefix).await.unwrap());
    let url = storage.url("claim").unwrap();
    let (a, b) = tokio::join!(
        storage.create(&url, "a".into()),
        storage.create(&url, "b".into())
    );
    assert_ne!(a.is_ok(), b.is_ok());
    assert!(storage.prefix_exists(&url).await.unwrap());
}

#[tokio::test]
async fn filesystem_urls_decode_and_preserve_siblings() {
    let temp = tempfile::tempdir().unwrap();
    let root = Url::from_directory_path(temp.path()).unwrap();
    let storage = Storage::from_url(&root).unwrap();
    let output = root.join("new/").unwrap();
    let file = output.join("part%20%E9%9B%AA").unwrap();
    storage.create(&file, "data".into()).await.unwrap();
    assert_eq!(storage.read(&file).await.unwrap(), "data");
    assert!(
        storage
            .stat_optional(&output)
            .await
            .unwrap()
            .unwrap()
            .is_dir()
    );
    let aborted = root.join("aborted").unwrap();
    let mut writer = storage.writer(&aborted).await.unwrap();
    writer.write("incomplete".into()).await.unwrap();
    writer.abort().await.unwrap();
    // Non-staged files can be visible before close; the owner removes them.
    storage.delete(&aborted).await.unwrap();
    let sibling = root.join("keep").unwrap();
    storage.create(&sibling, "keep".into()).await.unwrap();
    storage.remove_all(&output).await.unwrap();
    assert!(!storage.exists(&file).await.unwrap());
    assert_eq!(storage.read(&sibling).await.unwrap(), "keep");
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
    let (first, second) = tokio::join!(
        storage.create(&url, "first".into()),
        storage.create(&url, "second".into())
    );
    assert_ne!(first.is_ok(), second.is_ok());
    assert_eq!(
        storage.read(&url).await.unwrap(),
        if first.is_ok() { "first" } else { "second" }
    );
}

#[tokio::test]
async fn errors_retain_operation_path_and_opendal_source() {
    use std::error::Error as _;
    let storage = memory();
    let error = storage
        .read(&storage.url("missing").unwrap())
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
