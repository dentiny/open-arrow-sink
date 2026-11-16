//! Build indexes against the candidate manifest before its atomic publication.
use std::{fmt, sync::Arc};

use lance_io::object_store::ObjectStore;
use lance_table::{
    format::{IndexMetadata, Manifest, Transaction},
    io::commit::{
        CommitError, CommitHandler, ConditionalPutCommitHandler, ManifestLocation,
        ManifestNamingScheme, ManifestWriter,
    },
};
use object_store::{ObjectStore as DynObjectStore, ObjectStoreExt, path::Path};
use open_arrow_sink_core::{IndexSpec, async_trait};

use crate::{destination::LanceDestination, index::build_indexes};

pub(crate) struct IndexedCommitHandler {
    pub destination: LanceDestination,
    pub indexes: Vec<IndexSpec>,
}

impl fmt::Debug for IndexedCommitHandler {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("IndexedCommitHandler")
            .field("indexes", &self.indexes)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl CommitHandler for IndexedCommitHandler {
    fn is_version_not_found_definitive(&self) -> bool {
        true
    }

    fn propagate_commit_error_after_success(&self) -> bool {
        false
    }

    async fn commit(
        &self,
        manifest: &mut Manifest,
        indices: Option<Vec<IndexMetadata>>,
        base_path: &Path,
        object_store: &ObjectStore,
        manifest_writer: ManifestWriter,
        naming_scheme: ManifestNamingScheme,
        transaction: Option<Transaction>,
    ) -> Result<ManifestLocation, CommitError> {
        // Fragment and stable row IDs have now been assigned by Lance. Keep the
        // candidate outside _versions so ordinary readers cannot discover it.
        // Each attempt has its own staging file and isolated read caches.
        let path = base_path
            .clone()
            .join("_index_staging")
            .join(format!("{}.manifest", uuid::Uuid::new_v4()));
        let mut candidate = manifest.clone();
        let written = manifest_writer(
            object_store,
            &mut candidate,
            indices.clone(),
            &path,
            transaction.clone(),
        )
        .await?;
        let location = ManifestLocation {
            version: candidate.version,
            path: path.clone(),
            size: Some(written.size as u64),
            naming_scheme,
            e_tag: written.e_tag,
            identity: None,
        };
        let result = async {
            let mut dataset = self
                .destination
                .dataset_builder()
                .with_session(self.destination.isolated_session())
                .with_commit_handler(Arc::new(StagedManifest(location)))
                .with_version(candidate.version)
                .load()
                .await?;
            let built = build_indexes(&mut dataset, &self.indexes).await?;
            let mut indices = indices.unwrap_or_default();
            for index in built {
                indices.retain(|existing| existing.name != index.name);
                indices.push(index);
            }
            // Only this call makes data and all requested indexes visible.
            // If it conflicts, Lance rebuilds the next candidate and calls us
            // again: index row addresses must never survive a fragment-ID rebase.
            ConditionalPutCommitHandler
                .commit(
                    manifest,
                    Some(indices),
                    base_path,
                    object_store,
                    manifest_writer,
                    naming_scheme,
                    transaction,
                )
                .await
        }
        .await;
        // Cleanup cannot turn a successful publication into a reported failure.
        // Cancellation can leave staging/index files; they are never published.
        let _ = object_store.inner.delete(&path).await;
        result
    }
}

/// A private read-only view of one candidate; never publishes a version.
#[derive(Debug)]
struct StagedManifest(ManifestLocation);

#[async_trait]
impl CommitHandler for StagedManifest {
    async fn resolve_version_location(
        &self,
        base_path: &Path,
        version: u64,
        object_store: &dyn DynObjectStore,
    ) -> lance::Result<ManifestLocation> {
        if version == self.0.version {
            Ok(self.0.clone())
        } else {
            ConditionalPutCommitHandler
                .resolve_version_location(base_path, version, object_store)
                .await
        }
    }

    async fn commit(
        &self,
        _manifest: &mut Manifest,
        _indices: Option<Vec<IndexMetadata>>,
        _base_path: &Path,
        _object_store: &ObjectStore,
        _manifest_writer: ManifestWriter,
        _naming_scheme: ManifestNamingScheme,
        _transaction: Option<Transaction>,
    ) -> Result<ManifestLocation, CommitError> {
        Err(lance::Error::invalid_input("cannot commit a staged index snapshot").into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::committer::LanceCommitter;
    use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
    use lance::{
        dataset::{InsertBuilder, WriteMode as NativeMode, WriteParams},
        index::DatasetIndexExt,
    };
    use open_arrow_sink_core::{ExistingIndexPolicy, IndexType, SessionCommitter, WriteOptions};
    use open_arrow_sink_storage::{Storage, StorageResolver, opendal};

    fn batch() -> RecordBatch {
        RecordBatch::try_from_iter([
            ("id", Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef),
            (
                "text",
                Arc::new(StringArray::from(vec![
                    "hello world",
                    "hello lance",
                    "world",
                ])) as ArrayRef,
            ),
        ])
        .unwrap()
    }

    fn destination() -> LanceDestination {
        let storage = Storage::new(
            opendal::Operator::new(opendal::services::Memory::default()).unwrap(),
            "memory:///warehouse/".parse().unwrap(),
        )
        .unwrap();
        let url = storage
            .url(&open_arrow_sink_storage::ObjectPath::parse("atomic/").unwrap())
            .unwrap();
        LanceDestination::new(
            storage,
            url,
            Arc::new(StorageResolver::default()),
            Arc::new(open_arrow_sink_storage::BlobDownloader::with_defaults().unwrap()),
            None,
        )
        .unwrap()
    }

    async fn commit(
        destination: &LanceDestination,
        mode: NativeMode,
        indexes: Vec<IndexSpec>,
    ) -> open_arrow_sink_core::Result<()> {
        let params = destination.bind_write_params(WriteParams {
            mode,
            ..Default::default()
        });
        let transaction = InsertBuilder::new(destination.url.as_str())
            .with_params(&params)
            .execute_uncommitted(vec![batch()])
            .await
            .unwrap();
        Box::new(LanceCommitter {
            destination: destination.clone(),
            indexes,
            storage_version: None,
        })
        .commit(vec![crate::LanceFragments { transaction }])
        .await
    }

    fn indexes(policy: ExistingIndexPolicy) -> Vec<IndexSpec> {
        vec![
            IndexSpec::new("id", IndexType::Scalar).with_existing_policy(policy),
            IndexSpec::new("text", IndexType::Text).with_existing_policy(policy),
        ]
    }

    #[tokio::test]
    async fn data_and_all_indexes_publish_one_version() {
        let destination = destination();
        commit(
            &destination,
            NativeMode::Create,
            indexes(ExistingIndexPolicy::Error),
        )
        .await
        .unwrap();
        let dataset = destination.dataset_builder().load().await.unwrap();
        assert_eq!(dataset.version_id(), 1);
        assert_eq!(dataset.versions().await.unwrap().len(), 1);
        assert_eq!(dataset.count_rows(None).await.unwrap(), 3);
        assert_eq!(dataset.load_indices().await.unwrap().len(), 2);
        // Queries must be able to use the newly published scalar index.
        assert_eq!(
            dataset
                .scan()
                .filter("id = 2")
                .unwrap()
                .try_into_batch()
                .await
                .unwrap()
                .num_rows(),
            1
        );

        commit(
            &destination,
            NativeMode::Append,
            indexes(ExistingIndexPolicy::Rebuild),
        )
        .await
        .unwrap();
        let dataset = destination.dataset_builder().load().await.unwrap();
        assert_eq!(dataset.version_id(), 2);
        assert_eq!(dataset.count_rows(None).await.unwrap(), 6);
        assert_eq!(dataset.load_indices().await.unwrap().len(), 2);
        assert_eq!(
            dataset
                .scan()
                .filter("id = 2")
                .unwrap()
                .try_into_batch()
                .await
                .unwrap()
                .num_rows(),
            2
        );

        let existing = dataset.load_indices().await.unwrap();
        commit(
            &destination,
            NativeMode::Append,
            indexes(ExistingIndexPolicy::Keep),
        )
        .await
        .unwrap();
        let dataset = destination.dataset_builder().load().await.unwrap();
        assert_eq!(dataset.version_id(), 3);
        assert_eq!(*dataset.load_indices().await.unwrap(), *existing);
        assert_eq!(dataset.count_rows(None).await.unwrap(), 9);
        assert_eq!(
            dataset
                .scan()
                .filter("id = 2")
                .unwrap()
                .try_into_batch()
                .await
                .unwrap()
                .num_rows(),
            3
        );
    }

    #[tokio::test]
    async fn vector_and_bloom_indexes_are_published_with_the_data() {
        use crate::LanceDatasetSink;
        use arrow_array::{FixedSizeListArray, types::Float32Type};
        use open_arrow_sink_core::Sink;
        let destination = destination();
        let batch = RecordBatch::try_from_iter([
            (
                "id",
                Arc::new(Int64Array::from_iter_values(0..32)) as ArrayRef,
            ),
            (
                "vector",
                Arc::new(
                    FixedSizeListArray::from_iter_primitive::<Float32Type, _, _>(
                        (0..32).map(|i| Some(vec![Some(i as f32), Some((i * 2) as f32)])),
                        2,
                    ),
                ) as ArrayRef,
            ),
        ])
        .unwrap();
        let sink = LanceDatasetSink::new(destination.storage.clone());
        let mut writer = sink
            .open(
                destination.url.clone(),
                batch.schema(),
                WriteOptions::default().with_indexes(vec![
                    IndexSpec::new("id", IndexType::BloomFilter),
                    IndexSpec::new("vector", IndexType::Vector),
                ]),
            )
            .await
            .unwrap();
        writer.write(batch).await.unwrap();
        writer.commit().await.unwrap();
        let dataset = destination.dataset_builder().load().await.unwrap();
        assert_eq!(dataset.version_id(), 1);
        assert_eq!(dataset.count_rows(None).await.unwrap(), 32);
        assert_eq!(dataset.load_indices().await.unwrap().len(), 2);
        assert_eq!(
            dataset
                .scan()
                .filter("id = 7")
                .unwrap()
                .try_into_batch()
                .await
                .unwrap()
                .num_rows(),
            1
        );
        assert_eq!(
            dataset
                .scan()
                .nearest(
                    "vector",
                    &arrow_array::Float32Array::from(vec![7.0, 14.0]),
                    1
                )
                .unwrap()
                .try_into_batch()
                .await
                .unwrap()
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            7
        );
    }

    #[tokio::test]
    async fn concurrent_appends_rebuild_indexes_after_fragment_ids_change() {
        let destination = destination();
        commit(
            &destination,
            NativeMode::Create,
            indexes(ExistingIndexPolicy::Error),
        )
        .await
        .unwrap();
        let params = destination.bind_write_params(WriteParams {
            mode: NativeMode::Append,
            ..Default::default()
        });
        let first = InsertBuilder::new(destination.url.as_str())
            .with_params(&params)
            .execute_uncommitted(vec![batch()])
            .await
            .unwrap();
        let second = InsertBuilder::new(destination.url.as_str())
            .with_params(&params)
            .execute_uncommitted(vec![batch()])
            .await
            .unwrap();
        assert_eq!(first.read_version, second.read_version);
        let committer = || {
            Box::new(LanceCommitter {
                destination: destination.clone(),
                indexes: indexes(ExistingIndexPolicy::Rebuild),
                storage_version: None,
            })
        };
        let (first, second) = tokio::join!(
            committer().commit(vec![crate::LanceFragments { transaction: first }]),
            committer().commit(vec![crate::LanceFragments {
                transaction: second
            }])
        );
        first.unwrap();
        second.unwrap();
        let dataset = destination.dataset_builder().load().await.unwrap();
        assert_eq!(dataset.version_id(), 3);
        assert_eq!(dataset.count_rows(None).await.unwrap(), 9);
        assert_eq!(dataset.load_indices().await.unwrap().len(), 2);
        assert_eq!(
            dataset
                .scan()
                .filter("id = 2")
                .unwrap()
                .try_into_batch()
                .await
                .unwrap()
                .num_rows(),
            3
        );
        for index in dataset.load_indices().await.unwrap().iter() {
            assert_eq!(index.fragment_bitmap.as_ref().unwrap().len(), 3);
        }
    }

    #[rstest::rstest]
    #[case::create(NativeMode::Create)]
    #[case::append(NativeMode::Append)]
    #[case::overwrite(NativeMode::Overwrite)]
    #[tokio::test]
    async fn index_failure_publishes_neither_data_nor_indexes(#[case] mode: NativeMode) {
        let destination = destination();
        let existing = if !matches!(mode, NativeMode::Create) {
            commit(
                &destination,
                NativeMode::Create,
                indexes(ExistingIndexPolicy::Error),
            )
            .await
            .unwrap();
            Some(
                destination
                    .dataset_builder()
                    .load()
                    .await
                    .unwrap()
                    .load_indices()
                    .await
                    .unwrap(),
            )
        } else {
            None
        };
        // Exercise a native construction failure after the first index has been
        // built. Bypass open-time validation to reach the commit failure path.
        let specs = vec![
            IndexSpec::new("id", IndexType::Scalar)
                .with_existing_policy(ExistingIndexPolicy::Rebuild),
            IndexSpec::new("missing", IndexType::Scalar),
        ];
        let error = commit(&destination, mode, specs).await.unwrap_err();
        assert!(error.to_string().contains("missing"), "{error}");
        assert!(!error.to_string().contains("data committed"));
        let result = destination.dataset_builder().load().await;
        if let Some(existing) = &existing {
            let dataset = result.unwrap();
            assert_eq!(dataset.version_id(), 1);
            assert_eq!(dataset.count_rows(None).await.unwrap(), 3);
            assert_eq!(
                dataset.load_indices().await.unwrap().as_ref(),
                existing.as_ref()
            );
        } else {
            assert!(matches!(
                result,
                Err(lance::Error::DatasetNotFound { .. } | lance::Error::NotFound { .. })
            ));
        }
        // A failed construction must not consume a visible version.
        let retry_mode = if existing.is_some() {
            NativeMode::Append
        } else {
            NativeMode::Create
        };
        commit(
            &destination,
            retry_mode,
            indexes(ExistingIndexPolicy::Rebuild),
        )
        .await
        .unwrap();
        let dataset = destination.dataset_builder().load().await.unwrap();
        assert_eq!(
            dataset.count_rows(None).await.unwrap(),
            if existing.is_some() { 6 } else { 3 }
        );
        assert_eq!(dataset.version_id(), if existing.is_some() { 2 } else { 1 });
    }
}
