//! Resolve independent source namespaces without reusing destination credentials.
use crate::{Storage, Url};
use open_arrow_sink_core::Result;
use std::sync::{Arc, Mutex};

/// Explicit stores take precedence over lazily cached URL-configured stores.
/// Register separate stores for sources requiring custom credentials or endpoints.
#[derive(Clone, Debug, Default)]
pub struct StorageResolver {
    configured: Vec<Storage>,
    cached: Arc<Mutex<Vec<Storage>>>,
}
impl StorageResolver {
    pub fn with_storage(mut self, storage: Storage) -> Self {
        self.configured.push(storage);
        self.configured
            .sort_by_key(|s| std::cmp::Reverse(s.base_url().path().len()));
        self
    }
    pub fn schemes(&self) -> impl Iterator<Item = &str> {
        self.configured.iter().map(|s| s.base_url().scheme())
    }
    pub fn resolve(&self, url: &Url) -> Result<Storage> {
        for storage in &self.configured {
            if storage.path(url).is_ok() {
                return Ok(storage.clone());
            }
        }
        let mut cached = self
            .cached
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for storage in cached.iter() {
            if storage.path(url).is_ok() {
                return Ok(storage.clone());
            }
        }
        let storage = Storage::from_url(url)?;
        cached.push(storage.clone());
        Ok(storage)
    }
}
