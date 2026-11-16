use open_arrow_sink_core::{CompressionOptions, Layer, Sink};

use crate::{OptionsLayer, OptionsSink};

/// Overrides the entire codec/structural policy. The backend performs encoding.
/// Like OrderingLayer, install one compression layer per sink.
#[derive(Debug, Clone)]
pub struct CompressionLayer {
    compression: CompressionOptions,
}

impl CompressionLayer {
    pub fn new(compression: CompressionOptions) -> Self {
        Self { compression }
    }
}

pub type CompressionSink<S> = OptionsSink<S>;

impl<S: Sink> Layer<S> for CompressionLayer {
    type Layered = CompressionSink<S>;

    fn layer(&self, inner: S) -> Self::Layered {
        let compression = self.compression.clone();
        OptionsLayer::new(move |options| options.compression = compression.clone()).layer(inner)
    }
}
