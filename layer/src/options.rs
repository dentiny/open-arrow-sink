use open_arrow_sink_core::{
    Layer, Result, SchemaRef, Sink, SinkCapability, WriteOptions, WriteSession, Writer,
};
use std::sync::Arc;

use crate::decorate::{Decorator, impl_sink};

type Policy = dyn Fn(&mut WriteOptions, SinkCapability) -> Result<()> + Send + Sync;

/// Overrides session options before the backend validates and opens the destination.
/// The same policy applies to both single-writer and parallel sessions.
#[derive(Clone)]
pub struct OptionsLayer {
    policy: Arc<Policy>,
}

impl OptionsLayer {
    pub fn new(update: impl Fn(&mut WriteOptions) + Send + Sync + 'static) -> Self {
        Self::checked(move |options, _| {
            update(options);
            Ok(())
        })
    }

    pub(crate) fn checked(
        policy: impl Fn(&mut WriteOptions, SinkCapability) -> Result<()> + Send + Sync + 'static,
    ) -> Self {
        Self {
            policy: Arc::new(policy),
        }
    }
}

pub struct OptionsSink<S> {
    inner: S,
    policy: Arc<Policy>,
}

impl<S: Sink> Layer<S> for OptionsLayer {
    type Layered = OptionsSink<S>;

    fn layer(&self, inner: S) -> Self::Layered {
        OptionsSink {
            inner,
            policy: self.policy.clone(),
        }
    }
}

impl_sink!(OptionsSink);

impl<S: Sink> Decorator for OptionsSink<S> {
    type Inner = S;
    type Context = ();

    fn inner(&self) -> &S {
        &self.inner
    }

    fn prepare(&self, _schema: &mut SchemaRef, options: &mut WriteOptions) -> Result<()> {
        (self.policy)(options, self.inner.capability())
    }

    fn wrap_writer(&self, (): (), writer: Box<dyn Writer>) -> Box<dyn Writer> {
        writer
    }

    fn wrap_session<T: Send + 'static>(&self, (): (), session: WriteSession<T>) -> WriteSession<T> {
        session
    }
}
