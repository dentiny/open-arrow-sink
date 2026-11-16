use std::num::NonZeroUsize;

use futures::future::join;
use object_store::{ObjectStoreExt, path::Path};
use open_arrow_sink_core::{UploadConfig, WriteOptions};
use open_arrow_sink_storage::{Storage, opendal};

#[tokio::test]
async fn native_multipart_preserves_order_and_shared_namespace() {
    let storage = Storage::new(
        opendal::Operator::new(opendal::services::Memory::default()).unwrap(),
        "memory:///warehouse/".parse().unwrap(),
    )
    .unwrap()
    .with_write_options(
        &WriteOptions::default().with_upload(
            UploadConfig::default()
                .with_part_size(NonZeroUsize::new(3))
                .with_concurrency(NonZeroUsize::new(2)),
        ),
    );
    let adapter = storage.clone().object_store();
    let path = Path::from("ordered%20parts");
    let mut upload = adapter.put_multipart(&path).await.unwrap();
    let first = upload.put_part("first".into());
    let second = upload.put_part("second".into());
    // Poll in reverse order, as native SDKs are free to do.
    let (second_result, first_result) = join(second, first).await;
    first_result.unwrap();
    second_result.unwrap();
    upload.complete().await.unwrap();
    assert_eq!(
        adapter.get(&path).await.unwrap().bytes().await.unwrap(),
        "firstsecond"
    );
    assert_eq!(
        storage
            .read(&storage.url("ordered%20parts").unwrap())
            .await
            .unwrap(),
        "firstsecond"
    );
    // Ordinary conditional puts still use the upstream adapter.
    assert!(
        adapter
            .put_opts(
                &path,
                "replace".into(),
                object_store::PutOptions::from(object_store::PutMode::Create)
            )
            .await
            .is_err()
    );
    assert_eq!(adapter.head(&path).await.unwrap().size, 11);
}

#[tokio::test]
async fn native_multipart_abort_does_not_publish() {
    let storage = Storage::new(
        opendal::Operator::new(opendal::services::Memory::default()).unwrap(),
        "memory:///warehouse/".parse().unwrap(),
    )
    .unwrap();
    let adapter = storage.object_store();
    let path = Path::from("aborted");
    let mut upload = adapter.put_multipart(&path).await.unwrap();
    upload.put_part("partial".into()).await.unwrap();
    upload.abort().await.unwrap();
    assert!(
        !storage
            .exists(&storage.url("aborted").unwrap())
            .await
            .unwrap()
    );
}
