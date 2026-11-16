use open_arrow_sink_core::Result;

use crate::{error, validation};

/// A validated, decoded path relative to a Storage namespace.
/// Resolve external URLs with `Storage::resolve` before performing I/O. Paths
/// contain no endpoint or credentials; use them with the storage that resolved them.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ObjectPath(String);

impl ObjectPath {
    pub(crate) fn new(path: String) -> Self {
        Self(path)
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
        validation::validate_path(child)?;
        if child.starts_with('/') {
            return Err(error::invalid_path());
        }
        Ok(Self(format!("{}{child}", self.as_dir().as_str())))
    }

    pub(crate) fn require_nonroot(&self) -> Result<()> {
        if self.is_root() {
            return Err(error::empty_path());
        }
        Ok(())
    }
}
