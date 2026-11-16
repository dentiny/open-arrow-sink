//! Resolve independent source namespaces without reusing destination credentials.
use crate::{Storage, StorageConfig, Url};
use open_arrow_sink_core::Result;
use std::sync::{Arc, Mutex};

/// Map source URLs to independently configured storage namespaces.
///
/// Explicit stores take precedence over lazily cached URL-configured stores.
/// Register separate stores for sources requiring custom credentials or endpoints.
/// Resolution selects an operator; callers remain responsible for transferring
/// bytes and choosing where to write them. No destination configuration is used.
///
/// Clones share the URL-derived cache but retain their own explicit registrations.
#[derive(Clone, Debug, Default)]
pub struct StorageResolver {
    configured: Vec<Storage>,
    config: StorageConfig,
    cached: Arc<Mutex<Vec<Storage>>>,
}
impl StorageResolver {
    /// Configure policies for URL-derived stores. Explicit stores keep their
    /// own policies. Clones may use different policies while sharing the cache.
    pub fn with_config(mut self, config: StorageConfig) -> Result<Self> {
        config.validate()?;
        self.config = config;
        Ok(self)
    }

    /// Register a source namespace. More specific matching roots take precedence;
    /// equal roots retain registration order (the first registered store wins).
    pub fn with_storage(mut self, storage: Storage) -> Self {
        self.configured.push(storage);
        self.configured
            .sort_by_key(|s| std::cmp::Reverse(s.base_url().path().len()));
        self
    }
    /// Schemes accepted by OpenDAL or explicitly configured namespaces.
    /// Register custom OpenDAL services before constructing an upstream adapter;
    /// adapters such as Lance snapshot these schemes when creating a session.
    pub fn schemes(&self) -> impl Iterator<Item = String> {
        opendal::init_default_registry();
        let mut schemes = opendal::OperatorRegistry::get().schemes();
        schemes.extend(
            self.configured
                .iter()
                .map(|s| s.base_url().scheme().to_owned()),
        );
        schemes.into_iter()
    }
    /// Select explicit configuration first, then reuse or create URL-derived
    /// storage. URL-derived cloud storage requires the corresponding feature.
    pub fn resolve(&self, url: &Url) -> Result<Storage> {
        if let Some(storage) = matching_storage(&self.configured, url) {
            return Ok(storage);
        }
        let mut cached = self
            .cached
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(storage) = matching_storage(&cached, url) {
            return storage.with_config(self.config.clone());
        }
        let storage = Storage::from_url(url)?;
        // Cache the backend without resolver policies so differently configured
        // clones share data and HTTP budgets without inheriting each other's layers.
        let configured = storage.clone().with_config(self.config.clone())?;
        cached.push(storage);
        Ok(configured)
    }
}

fn matching_storage(stores: &[Storage], url: &Url) -> Option<Storage> {
    stores
        .iter()
        .find(|storage| storage.path(url).is_ok())
        .cloned()
}
