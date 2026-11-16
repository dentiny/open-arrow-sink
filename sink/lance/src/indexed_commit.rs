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
        manifest_writer(
            object_store,
            manifest,
            indices.clone(),
            &path,
            transaction.clone(),
        )
        .await?;
        let location = ManifestLocation {
            version: manifest.version,
            path: path.clone(),
            size: None,
            naming_scheme,
            e_tag: None,
            identity: None,
        };
        let result = async {
            let mut dataset = self
                .destination
                .dataset_builder()
                .with_session(self.destination.isolated_session())
                .with_commit_handler(Arc::new(StagedManifest(location)))
                .with_version(manifest.version)
                .load()
                .await?;
            let indices =
                build_indexes(&mut dataset, &self.indexes, indices.unwrap_or_default()).await?;
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
    use crate::{LanceFragments, committer::LanceCommitter};
    use arrow_array::{
        ArrayRef, FixedSizeListArray, Float32Array, Int64Array, RecordBatch, StringArray,
        types::Float32Type,
    };
    use lance::{
        Dataset,
        dataset::{InsertBuilder, WriteMode, WriteParams},
        index::DatasetIndexExt,
    };
    use open_arrow_sink_core::{ExistingIndexPolicy as Policy, IndexType, SessionCommitter};
    use open_arrow_sink_storage::{BlobDownloader, StorageResolver};
    use open_arrow_sink_test_support::{memory_storage, path};

    fn batch() -> RecordBatch {
        let ids = Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef;
        RecordBatch::try_from_iter([
            ("id", ids.clone()),
            ("bloom", ids),
            (
                "text",
                Arc::new(StringArray::from(vec![
                    "hello world",
                    "hello lance",
                    "world",
                ])) as ArrayRef,
            ),
            (
                "vector",
                Arc::new(
                    FixedSizeListArray::from_iter_primitive::<Float32Type, _, _>(
                        (1..=3).map(|i| Some(vec![Some(i as f32), Some((i * 2) as f32)])),
                        2,
                    ),
                ) as ArrayRef,
            ),
        ])
        .unwrap()
    }

    fn indexes(policy: Policy) -> Vec<IndexSpec> {
        vec![
            IndexSpec::new("id", IndexType::Scalar),
            IndexSpec::new("text", IndexType::Text),
            IndexSpec::new("bloom", IndexType::BloomFilter),
            IndexSpec::new("vector", IndexType::Vector),
        ]
        .into_iter()
        .map(|spec| spec.with_existing_policy(policy))
        .collect()
    }

    fn destination() -> LanceDestination {
        let storage = memory_storage("memory:///warehouse/");
        let url = storage.url(&path(&storage, "atomic/")).unwrap();
        LanceDestination::new(
            storage,
            url,
            Arc::new(StorageResolver::default()),
            Arc::new(BlobDownloader::with_defaults().unwrap()),
            None,
        )
        .unwrap()
    }

    impl LanceDestination {
        async fn stage(&self, mode: WriteMode) -> LanceFragments {
            let params = self.bind_write_params(WriteParams {
                mode,
                ..Default::default()
            });
            LanceFragments {
                transaction: InsertBuilder::new(self.url.as_str())
                    .with_params(&params)
                    .execute_uncommitted(vec![batch()])
                    .await
                    .unwrap(),
            }
        }

        fn committer(&self, indexes: Vec<IndexSpec>) -> Box<LanceCommitter> {
            Box::new(LanceCommitter {
                destination: self.clone(),
                indexes,
                storage_version: None,
            })
        }

        async fn write(
            &self,
            mode: WriteMode,
            indexes: Vec<IndexSpec>,
        ) -> open_arrow_sink_core::Result<()> {
            self.committer(indexes)
                .commit(vec![self.stage(mode).await])
                .await
        }

        async fn snapshot(&self, version: u64, rows: usize) -> Arc<Vec<IndexMetadata>> {
            let dataset = self.dataset_builder().load().await.unwrap();
            assert_eq!(dataset.version_id(), version);
            assert_eq!(dataset.versions().await.unwrap().len(), version as usize);
            assert_eq!(dataset.count_rows(None).await.unwrap(), rows);
            assert_eq!(matching_rows(&dataset, "id = 2").await, rows / 3);
            let indexes = dataset.load_indices().await.unwrap();
            assert_eq!(indexes.len(), 4);
            assert_eq!(matching_rows(&dataset, "bloom = 2").await, rows / 3);
            let nearest = dataset
                .scan()
                .nearest("vector", &Float32Array::from(vec![2.0, 4.0]), 1)
                .unwrap()
                .try_into_batch()
                .await
                .unwrap();
            assert_eq!(
                nearest
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .value(0),
                2
            );
            indexes
        }
    }

    async fn matching_rows(dataset: &Dataset, filter: &str) -> usize {
        dataset
            .scan()
            .filter(filter)
            .unwrap()
            .try_into_batch()
            .await
            .unwrap()
            .num_rows()
    }

    #[tokio::test]
    async fn data_and_all_indexes_publish_one_version() {
        let dest = destination();
        dest.write(WriteMode::Create, indexes(Policy::Error))
            .await
            .unwrap();
        dest.snapshot(1, 3).await;
        dest.write(WriteMode::Append, indexes(Policy::Rebuild))
            .await
            .unwrap();
        let rebuilt = dest.snapshot(2, 6).await;
        dest.write(WriteMode::Append, indexes(Policy::Keep))
            .await
            .unwrap();
        assert_eq!(dest.snapshot(3, 9).await, rebuilt);
    }

    #[tokio::test]
    async fn concurrent_appends_rebuild_indexes_after_fragment_ids_change() {
        let dest = destination();
        dest.write(WriteMode::Create, indexes(Policy::Error))
            .await
            .unwrap();
        let first = dest.stage(WriteMode::Append).await;
        let second = dest.stage(WriteMode::Append).await;
        assert_eq!(
            first.transaction.read_version,
            second.transaction.read_version
        );
        let (first, second) = tokio::join!(
            dest.committer(indexes(Policy::Rebuild)).commit(vec![first]),
            dest.committer(indexes(Policy::Rebuild))
                .commit(vec![second]),
        );
        first.unwrap();
        second.unwrap();
        for index in dest.snapshot(3, 9).await.iter() {
            assert_eq!(index.fragment_bitmap.as_ref().unwrap().len(), 3);
        }
    }

    #[rstest::rstest]
    #[case::create(WriteMode::Create)]
    #[case::append(WriteMode::Append)]
    #[case::overwrite(WriteMode::Overwrite)]
    #[tokio::test]
    async fn index_failure_publishes_neither_data_nor_indexes(#[case] mode: WriteMode) {
        let dest = destination();
        let existing = if !matches!(mode, WriteMode::Create) {
            dest.write(WriteMode::Create, indexes(Policy::Error))
                .await
                .unwrap();
            Some(dest.snapshot(1, 3).await)
        } else {
            None
        };
        // Bypass open-time validation: fail native construction after one index was built.
        let error = dest
            .write(
                mode,
                vec![
                    IndexSpec::new("id", IndexType::Scalar).with_existing_policy(Policy::Rebuild),
                    IndexSpec::new("missing", IndexType::Scalar),
                ],
            )
            .await
            .unwrap_err();
        assert!(error.to_string().contains("missing"), "{error}");
        assert!(!error.to_string().contains("data committed"));
        if let Some(existing) = &existing {
            assert_eq!(&dest.snapshot(1, 3).await, existing);
        } else {
            assert!(matches!(
                dest.dataset_builder().load().await,
                Err(lance::Error::DatasetNotFound { .. } | lance::Error::NotFound { .. })
            ));
        }
        let (mode, version, rows) = if existing.is_some() {
            (WriteMode::Append, 2, 6)
        } else {
            (WriteMode::Create, 1, 3)
        };
        dest.write(mode, indexes(Policy::Rebuild)).await.unwrap();
        dest.snapshot(version, rows).await;
    }
}
