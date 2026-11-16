use open_arrow_sink_core::{Layer, Ordering, Sink, validation};

use crate::{OptionsLayer, OptionsSink};

/// Override the shared ordering policy for each session opened through this layer.
/// This declares permission/requirements; backends implement the execution strategy.
#[derive(Debug, Clone, Copy)]
pub struct OrderingLayer {
    ordering: Ordering,
}

impl OrderingLayer {
    pub fn new(ordering: Ordering) -> Self {
        Self { ordering }
    }
}

pub type OrderingSink<S> = OptionsSink<S>;

impl<S: Sink> Layer<S> for OrderingLayer {
    type Layered = OrderingSink<S>;

    fn layer(&self, inner: S) -> Self::Layered {
        let ordering = self.ordering;
        OptionsLayer::checked(move |options, capability| {
            options.ordering = ordering;
            // Permission to reorder does not create an ordering guarantee.
            validation::validate_ordering_options(options.ordering, capability.ordering)
        })
        .layer(inner)
    }
}
