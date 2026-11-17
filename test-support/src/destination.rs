//! Filesystem destinations shared by backend conformance fixtures.
use open_arrow_sink_storage::Url;
use std::path::{Path, PathBuf};

/// An isolated destination and an unrelated sibling, kept alive for a test.
pub struct TempDirDestination {
    temp: tempfile::TempDir,
    path: PathBuf,
}

impl TempDirDestination {
    /// Reserve a destination name without creating its directory.
    pub fn new(name: &str) -> Self {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join(name);
        Self { temp, path }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn url(&self) -> Url {
        Url::from_directory_path(&self.path).unwrap()
    }

    pub async fn create_sibling(&self) {
        tokio::fs::write(self.temp.path().join("keep.txt"), "keep")
            .await
            .unwrap();
    }

    pub async fn assert_sibling(&self) {
        assert_eq!(
            tokio::fs::read_to_string(self.temp.path().join("keep.txt"))
                .await
                .unwrap(),
            "keep"
        );
    }
}
