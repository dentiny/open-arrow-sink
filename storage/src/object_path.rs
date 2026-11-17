use open_arrow_sink_core::{Error, Result};

use crate::validation;

/// A validated, decoded path relative to a Storage namespace.
/// Convert external URLs with `Storage::path`, or parse decoded relative keys. Paths
/// contain no endpoint or credentials; use them with the storage that resolved them.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ObjectPath(String);

impl ObjectPath {
    pub(crate) fn new(path: String) -> Self {
        Self(path)
    }

    /// Parse a decoded path relative to a storage root, without URL decoding.
    /// Reject absolute paths and traversal; percent signs remain literal key bytes.
    pub fn parse(path: &str) -> Result<Self> {
        validate_relative(path)?;
        Ok(Self(path.to_owned()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn is_root(&self) -> bool {
        self.0.trim_matches('/').is_empty()
    }

    /// Give a prefix directory semantics, preserving the operator root.
    pub fn as_dir(&self) -> Self {
        if self.is_root() {
            Self(String::new())
        } else {
            Self(format!("{}/", self.0.trim_end_matches('/')))
        }
    }

    /// Address the exact key at this prefix, without a directory marker.
    pub fn as_object(&self) -> Self {
        Self(self.0.trim_end_matches('/').to_owned())
    }

    /// Append a decoded relative path beneath this directory.
    pub fn join(&self, child: &str) -> Result<Self> {
        validate_relative(child)?;
        Ok(Self(format!("{}{child}", self.as_dir().as_str())))
    }

    pub(crate) fn require_nonroot(&self) -> Result<()> {
        validation::validate_nonroot_path(self.as_str())
    }
}

fn validate_relative(path: &str) -> Result<()> {
    validation::validate_path(path)?;
    if path.starts_with('/') {
        return Err(Error::InvalidInput("invalid storage path".into()));
    }
    Ok(())
}
