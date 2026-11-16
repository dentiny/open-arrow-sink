//! Resolve independent source namespaces without reusing destination credentials.
use crate::{Storage, Url};
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
    cached: Arc<Mutex<Vec<Storage>>>,
}
impl StorageResolver {
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
            return Ok(storage);
        }
        let storage = Storage::from_url(url)?;
        cached.push(storage.clone());
        Ok(storage)
    }
}

fn matching_storage(stores: &[Storage], url: &Url) -> Option<Storage> {
    stores
        .iter()
        .find(|storage| storage.resolve(url).is_ok())
        .cloned()
}
