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
    let url = storage.url("雪 space/file").unwrap();
    let mut writer = storage.writer(&url).await.unwrap();
    writer.write("hello ".into()).await.unwrap();
    writer.write("world".into()).await.unwrap();
    assert_eq!(writer.close().await.unwrap(), 11);
    assert!(writer.write("late".into()).await.is_err());
    assert_eq!(storage.clone().read(&url).await.unwrap(), "hello world");
    assert_eq!(storage.read_range(&url, 6..11).await.unwrap(), "world");
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
        assert!(storage.path(&url.parse().unwrap()).is_err(), "{url}");
    }
    assert!(
        storage
            .read_range(&"memory:///outside/file".parse().unwrap(), 0..0)
            .await
            .is_err()
    );
    assert!(storage.url("../outside").is_err());
    assert!(storage.remove_all(storage.base_url()).await.is_err());
    let url = storage.url("claim").unwrap();
    let (a, b) = tokio::join!(
        storage.create(&url, "a".into()),
        storage.create(&url, "b".into())
    );
    assert_ne!(a.is_ok(), b.is_ok());
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
    assert!(config.apply(operator.clone()).is_err());
    let config = StorageConfig {
        retry: None,
        timeout: Some(TimeoutConfig {
            io_timeout: Duration::ZERO,
            ..Default::default()
        }),
    };
    assert!(config.apply(operator).is_err());
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

#[test]
fn config_validation_and_configured_construction_match_access() {
    let config = StorageConfig {
        retry: Some(RetryConfig {
            min_delay: Duration::from_secs(2),
            max_delay: Duration::from_secs(1),
            ..Default::default()
        }),
        timeout: None,
    };
    assert!(
        config
            .validate()
            .unwrap_err()
            .to_string()
            .contains("retry min_delay must not exceed max_delay")
    );
    Storage::try_new(
        opendal::Operator::new(opendal::services::Memory::default()).unwrap(),
        "memory:///".parse().unwrap(),
        StorageConfig::default(),
    )
    .unwrap();
}
