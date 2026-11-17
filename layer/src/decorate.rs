//! Shared opening lifecycle for decorators that support both sink interfaces.
use open_arrow_sink_core::{
    ParallelSink, Result, SchemaRef, Sink, SinkCapability, WriteOptions, WriteSession, Writer,
};
use std::num::NonZeroUsize;

/// Shared preparation and wrapping hooks for single-writer and parallel opens.
pub(crate) trait Decorator: Send + Sync {
    /// Sink receiving the prepared schema and options.
    type Inner: Sink;
    /// Per-open state retained until the backend opens successfully or fails.
    type Context: Send;

    /// Borrow the backend or next layer receiving the open operation.
    fn inner(&self) -> &Self::Inner;

    /// Return the decorated sink's capabilities, forwarding the inner sink's
    /// capabilities by default. Override when the decorator changes guarantees.
    fn capability(&self) -> SinkCapability {
        self.inner().capability()
    }

    /// Preflight uses the same schema and option transformation as opening.
    fn check_schema(
        &self,
        schema: &arrow_schema::Schema,
        options: &WriteOptions,
    ) -> Vec<open_arrow_sink_core::SchemaIssue> {
        let mut schema = std::sync::Arc::new(schema.clone());
        let mut options = options.clone();
        match self.prepare(&mut schema, &mut options) {
            Ok(_) => self.inner().check_schema(&schema, &options),
            Err(error) => vec![open_arrow_sink_core::SchemaIssue::new(
                vec![],
                open_arrow_sink_core::SchemaIssueKind::InvalidOption,
                error.to_string(),
            )],
        }
    }

    /// Runs before either backend open. The context lives across the await,
    /// including failure and cancellation, and is consumed after a successful open.
    fn prepare(&self, schema: &mut SchemaRef, options: &mut WriteOptions) -> Result<Self::Context>;

    /// Consume preparation state to wrap a successfully opened single writer.
    fn wrap_writer(&self, context: Self::Context, writer: Box<dyn Writer>) -> Box<dyn Writer>;

    /// Consume preparation state to wrap a successfully opened parallel session,
    /// preserving its backend output type and shared publication boundary.
    fn wrap_session<T: Send + 'static>(
        &self,
        context: Self::Context,
        session: WriteSession<T>,
    ) -> WriteSession<T>;
}

pub(crate) async fn open<D: Decorator>(
    decorator: &D,
    request: <D::Inner as Sink>::Request,
    mut schema: SchemaRef,
    mut options: WriteOptions,
) -> Result<Box<dyn Writer>> {
    let context = decorator.prepare(&mut schema, &mut options)?;
    let writer = decorator.inner().open(request, schema, options).await?;
    Ok(decorator.wrap_writer(context, writer))
}

pub(crate) async fn open_parallel<D: Decorator>(
    decorator: &D,
    request: <D::Inner as Sink>::Request,
    mut schema: SchemaRef,
    mut options: WriteOptions,
    count: NonZeroUsize,
) -> Result<WriteSession<<D::Inner as ParallelSink>::Output>>
where
    D::Inner: ParallelSink,
{
    let context = decorator.prepare(&mut schema, &mut options)?;
    let session = decorator
        .inner()
        .open_writers(request, schema.clone(), options, count)
        .await?
        .expect(count.get(), &schema)?;
    Ok(decorator.wrap_session(context, session))
}

// Keep the concrete public wrapper types and their conditional ParallelSink
// support. Only interface forwarding is generated; each decorator defines its
// preparation and wrapping policy once through Decorator.
macro_rules! impl_sink {
    ($wrapper:ident) => {
        #[open_arrow_sink_core::async_trait]
        impl<S: open_arrow_sink_core::Sink> open_arrow_sink_core::Sink for $wrapper<S> {
            type Request = S::Request;

            fn capability(&self) -> open_arrow_sink_core::SinkCapability {
                crate::decorate::Decorator::capability(self)
            }

            fn check_schema(
                &self,
                schema: &arrow_schema::Schema,
                options: &open_arrow_sink_core::WriteOptions,
            ) -> Vec<open_arrow_sink_core::SchemaIssue> {
                crate::decorate::Decorator::check_schema(self, schema, options)
            }

            async fn open(
                &self,
                request: Self::Request,
                schema: open_arrow_sink_core::SchemaRef,
                options: open_arrow_sink_core::WriteOptions,
            ) -> open_arrow_sink_core::Result<Box<dyn open_arrow_sink_core::Writer>> {
                crate::decorate::open(self, request, schema, options).await
            }
        }

        #[open_arrow_sink_core::async_trait]
        impl<S: open_arrow_sink_core::ParallelSink> open_arrow_sink_core::ParallelSink
            for $wrapper<S>
        {
            type Output = S::Output;

            async fn open_writers(
                &self,
                request: Self::Request,
                schema: open_arrow_sink_core::SchemaRef,
                options: open_arrow_sink_core::WriteOptions,
                count: std::num::NonZeroUsize,
            ) -> open_arrow_sink_core::Result<open_arrow_sink_core::WriteSession<Self::Output>>
            {
                crate::decorate::open_parallel(self, request, schema, options, count).await
            }
        }
    };
}
pub(crate) use impl_sink;
