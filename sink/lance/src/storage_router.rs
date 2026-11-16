//! Select storage independently of Lance's object-store adapter.
use open_arrow_sink_core::Result;
use open_arrow_sink_storage::{Storage, StorageResolver, Url};

/// The destination owns its namespace; all other URLs resolve as sources.
/// Keeping this policy here prevents source registration from replacing the
/// destination's credentials, endpoint, or operator.
#[derive(Debug)]
pub(crate) struct StorageRouter {
    destination: Storage,
    sources: StorageResolver,
}

impl StorageRouter {
    pub fn new(destination: Storage, sources: StorageResolver) -> Self {
        Self {
            destination,
            sources,
        }
    }

    /// Adapt storage capabilities to Lance's per-scheme provider registry.
    pub fn schemes(&self) -> impl Iterator<Item = String> {
        self.sources.schemes().chain(std::iter::once(
            self.destination.base_url().scheme().to_owned(),
        ))
    }

    pub fn resolve(&self, url: &Url) -> Result<Storage> {
        if self.destination.resolve(url).is_ok() {
            Ok(self.destination.clone())
        } else {
            self.sources.resolve(url)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use open_arrow_sink_storage::opendal;

    fn memory(base: &str) -> Storage {
        Storage::new(
            opendal::Operator::new(opendal::services::Memory::default()).unwrap(),
            base.parse().unwrap(),
        )
        .unwrap()
    }

    async fn read_from(storage: Storage, url: &Url) -> bytes::Bytes {
        let path = storage.resolve(url).unwrap();
        storage.read(&path).await.unwrap()
    }

    #[tokio::test]
    async fn destination_owns_its_namespace_even_with_a_more_specific_source() {
        let destination = memory("s3://destination/");
        let overlap = memory("s3://destination/nested/");
        let external = memory("s3://source/");
        let local_url = destination.url("nested/key").unwrap();
        let external_url = external.url("key").unwrap();
        destination
            .write(
                &destination.resolve(&local_url).unwrap(),
                "destination".into(),
            )
            .await
            .unwrap();
        overlap
            .write(&overlap.resolve(&local_url).unwrap(), "overlap".into())
            .await
            .unwrap();
        external
            .write(&external.resolve(&external_url).unwrap(), "source".into())
            .await
            .unwrap();

        let router = StorageRouter::new(
            destination,
            StorageResolver::default()
                .with_storage(overlap)
                .with_storage(external),
        );
        assert_eq!(
            read_from(router.resolve(&local_url).unwrap(), &local_url).await,
            "destination"
        );
        assert_eq!(
            read_from(router.resolve(&external_url).unwrap(), &external_url).await,
            "source"
        );
    }
}
