//! Explicitly select URL columns for the backend's bounded blob downloader.
use std::{collections::HashSet, sync::Arc};

use arrow_schema::DataType;
use open_arrow_sink_core::{
    BLOB_METADATA_KEY, BLOB_METADATA_VALUE, Error, Layer, Result, SchemaRef, Sink, SinkCapability,
    WriteOptions, WriteSession, Writer, validation,
};

use crate::{
    decorate::{Decorator, impl_sink},
    schema_evolution::{FieldRemap, Plan},
};

/// Interpret only the selected top-level text columns as blob URLs.
/// Unselected strings stay strings. The backend owns downloads, resource limits,
/// and blob layout; this layer neither downloads nor buffers payloads itself.
#[derive(Clone)]
pub struct DownloadBlobLayer {
    columns: Arc<HashSet<String>>,
}

impl DownloadBlobLayer {
    pub fn new(columns: HashSet<String>) -> Self {
        Self {
            columns: Arc::new(columns),
        }
    }
}

pub struct DownloadBlobSink<S> {
    inner: S,
    columns: Arc<HashSet<String>>,
}

impl<S: Sink> Layer<S> for DownloadBlobLayer {
    type Layered = DownloadBlobSink<S>;

    fn layer(&self, inner: S) -> Self::Layered {
        DownloadBlobSink {
            inner,
            columns: self.columns.clone(),
        }
    }
}

impl_sink!(DownloadBlobSink);

impl<S: Sink> Decorator for DownloadBlobSink<S> {
    type Inner = S;
    type Context = Plan;

    fn inner(&self) -> &S {
        &self.inner
    }

    fn capability(&self) -> SinkCapability {
        self.inner.capability().with_supports_blob_streaming(false)
    }

    fn prepare(&self, schema: &mut SchemaRef, _: &mut WriteOptions) -> Result<Plan> {
        if !self.columns.is_empty() && !self.inner.capability().supports_blob_handle {
            return Err(Error::Unsupported("blob URL inputs".into()));
        }
        let mut remaps = Vec::with_capacity(self.columns.len());
        for name in self.columns.iter() {
            let index = validation::unique_column_index(schema, name).ok_or_else(|| {
                Error::InvalidInput(format!("blob URL column must exist and be unique: {name}"))
            })?;
            let field = schema.field(index);
            if !matches!(
                field.data_type(),
                DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
            ) {
                return Err(Error::InvalidInput(format!(
                    "blob URL column must be text: {name}"
                )));
            }
            let mut metadata = field.metadata().clone();
            metadata.insert(BLOB_METADATA_KEY.into(), BLOB_METADATA_VALUE.into());
            remaps.push(FieldRemap::new(
                name,
                field.clone().with_metadata(metadata),
                Ok,
            ));
        }
        let plan = Plan::new(schema, &remaps)?;
        // Keep the caller's input schema on the outer writer. The metadata-only
        // transform shares arrays, so it cannot reserve a payload twice.
        *schema = plan.target.clone();
        Ok(plan)
    }

    fn wrap_writer(&self, plan: Plan, inner: Box<dyn Writer>) -> Box<dyn Writer> {
        plan.wrap_writer(inner)
    }

    fn wrap_session<T: Send + 'static>(
        &self,
        plan: Plan,
        session: WriteSession<T>,
    ) -> WriteSession<T> {
        plan.wrap_session(session)
    }
}
