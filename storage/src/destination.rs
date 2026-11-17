//! Shared destination namespace resolution for persistent sinks.
use open_arrow_sink_core::Result;

use crate::{Storage, StorageConfig, StorageResolver, Url};

/// Resolve destination storage while preserving explicit operators and shared budgets.
///
/// An explicit storage always takes precedence over URL-derived storage. Clones share
/// URL-derived backends and HTTP budgets; each clone retains its own retry/timeout
/// configuration. Session upload options and observers can be applied to the resolved
/// storage with [`Storage::with_write_options`].
#[derive(Clone, Debug, Default)]
pub struct DestinationResolver {
    explicit: Option<Storage>,
    resolver: StorageResolver,
    config: StorageConfig,
}

impl DestinationResolver {
    /// Reuse a configured operator, including credentials, transport, and layers.
    pub fn new(storage: Storage) -> Self {
        Self {
            explicit: Some(storage),
            ..Self::default()
        }
    }

    /// Configure retry/timeout policies for explicit or URL-derived storage.
    /// Validation happens immediately, without destination I/O. Source stores are
    /// independent of this configuration.
    pub fn with_config(mut self, config: StorageConfig) -> Result<Self> {
        config.validate()?;
        self.config = config;
        Ok(self)
    }

    /// Select the destination operator and apply this clone's policies without I/O.
    /// Explicit operators must address the URL's namespace; callers can validate
    /// and convert the URL to a relative key with [`Storage::path`].
    pub fn resolve(&self, url: &Url) -> Result<Storage> {
        match &self.explicit {
            Some(storage) => storage.clone(),
            None => self.resolver.storage_for(url)?,
        }
        .with_config(self.config.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use open_arrow_sink_core::{Error, IoMetrics};
    use std::time::Duration;

    #[tokio::test]
    async fn explicit_operator_and_observers_are_preserved() {
        let temp = tempfile::tempdir().unwrap();
        let url = Url::from_file_path(temp.path().join("key")).unwrap();
        let operator = opendal::Operator::new(opendal::services::Memory::default()).unwrap();
        let storage =
            Storage::new(operator, Url::from_directory_path(temp.path()).unwrap()).unwrap();
        let metrics = IoMetrics::default();
        let storage = storage.with_io_metrics(metrics.clone());
        let destination = DestinationResolver::new(storage.clone());
        let resolved = destination.resolve(&url).unwrap();
        assert!(
            resolved
                .http_concurrency_limit()
                .shares_budget(&storage.http_concurrency_limit())
        );
        let path = resolved.path(&url).unwrap();
        resolved.write(&path, "explicit".into()).await.unwrap();
        assert_eq!(storage.read(&path).await.unwrap(), "explicit");
        assert_eq!(metrics.snapshot().bytes_written, 8);
        assert!(!temp.path().join("key").exists());
    }

    #[tokio::test]
    async fn clones_share_url_derived_backends_and_budgets() {
        let scheme = "destination-storage-test";
        opendal::OperatorRegistry::get().register::<opendal::services::Memory>(scheme);
        let url = Url::parse(&format!("{scheme}://destination/key")).unwrap();
        let destination = DestinationResolver::default();
        // Clone before populating the shared URL cache.
        let clone = destination.clone();
        let first = destination.resolve(&url).unwrap();
        let second = clone.resolve(&url).unwrap();
        assert!(
            first
                .http_concurrency_limit()
                .shares_budget(&second.http_concurrency_limit())
        );
        first
            .write(&first.path(&url).unwrap(), "shared".into())
            .await
            .unwrap();
        assert_eq!(
            second.read(&second.path(&url).unwrap()).await.unwrap(),
            "shared"
        );
    }

    #[test]
    fn rejects_invalid_policies_before_resolving_storage() {
        let config = StorageConfig {
            timeout: Some(crate::TimeoutConfig {
                operation_timeout: Duration::ZERO,
                ..Default::default()
            }),
            ..Default::default()
        };
        assert!(matches!(
            DestinationResolver::default().with_config(config),
            Err(Error::InvalidConfig(_))
        ));
    }
}
