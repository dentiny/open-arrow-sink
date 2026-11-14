//! Optional sink decorators.
mod ordering;
pub use ordering::{OrderingLayer, OrderingSink};

#[cfg(feature = "rebatch")]
mod rebatch;
#[cfg(feature = "rebatch")]
pub use rebatch::{RebatchLayer, RebatchSink};

#[cfg(feature = "download-blob")]
mod download_blob;
#[cfg(feature = "download-blob")]
pub use download_blob::{DownloadBlobLayer, DownloadBlobSink};
