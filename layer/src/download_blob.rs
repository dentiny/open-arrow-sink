//! Explicitly select URL columns for the backend's bounded blob downloader.
use std::{collections::HashSet, sync::Arc};

use arrow_schema::DataType;
use open_arrow_sink_core::{
    BLOB_METADATA_KEY, BLOB_METADATA_VALUE, DownloadFailurePolicy, DownloadMetrics,
    DownloadRecovery, Error, Layer, Result, SchemaRef, Sink, SinkCapability, WriteOptions,
    WriteSession, Writer, validation,
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
    failure_policy: DownloadFailurePolicy,
    metrics: DownloadMetrics,
}

impl DownloadBlobLayer {
    pub fn with_failure_policy(mut self, policy: DownloadFailurePolicy) -> Self {
        self.failure_policy = policy;
        self
    }
    pub fn metrics(&self) -> DownloadMetrics {
        self.metrics.clone()
    }
    pub fn new(columns: HashSet<String>) -> Self {
        Self {
            columns: Arc::new(columns),
            failure_policy: DownloadFailurePolicy::Fail,
            metrics: DownloadMetrics::default(),
        }
    }
}

pub struct DownloadBlobSink<S> {
    inner: S,
    columns: Arc<HashSet<String>>,
    failure_policy: DownloadFailurePolicy,
    metrics: DownloadMetrics,
}

impl<S: Sink> Layer<S> for DownloadBlobLayer {
    type Layered = DownloadBlobSink<S>;

    fn layer(&self, inner: S) -> Self::Layered {
        DownloadBlobSink {
            inner,
            columns: self.columns.clone(),
            failure_policy: self.failure_policy,
            metrics: self.metrics.clone(),
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
        self.inner.capability()
    }

    fn check_schema(
        &self,
        schema: &arrow_schema::Schema,
        options: &WriteOptions,
    ) -> Vec<open_arrow_sink_core::SchemaIssue> {
        use open_arrow_sink_core::{SchemaIssue, SchemaIssueKind as Kind};
        let mut issues = Vec::new();
        let mut selected = self.columns.iter().collect::<Vec<_>>();
        selected.sort();
        let mut valid = HashSet::new();
        for name in selected {
            match validation::unique_column_index(schema, name) {
                None => issues.push(SchemaIssue::new(
                    vec![name.clone()],
                    Kind::MissingColumn,
                    "blob URL column must exist and be unique",
                )),
                Some(index) => {
                    if matches!(
                        schema.field(index).data_type(),
                        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
                    ) {
                        valid.insert(name.clone());
                    } else {
                        issues.push(SchemaIssue::new(
                            vec![name.clone()],
                            Kind::BlobType,
                            "blob URL column must be text",
                        ));
                    }
                }
            }
        }
        // Reuse the opening transformation for all valid selections, so downstream
        // checks see the actual blob metadata and nullability.
        let mut prepared_schema = Arc::new(schema.clone());
        let mut prepared_options = options.clone();
        match self.prepare_selected(&valid, &mut prepared_schema, &mut prepared_options) {
            Ok(_) => issues.extend(self.inner.check_schema(&prepared_schema, &prepared_options)),
            Err(error) => issues.push(SchemaIssue::new(
                vec![],
                Kind::InvalidOption,
                error.to_string(),
            )),
        }
        issues
    }

    fn prepare(&self, schema: &mut SchemaRef, options: &mut WriteOptions) -> Result<Plan> {
        self.prepare_selected(&self.columns, schema, options)
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

impl<S: Sink> DownloadBlobSink<S> {
    fn prepare_selected(
        &self,
        columns: &HashSet<String>,
        schema: &mut SchemaRef,
        options: &mut WriteOptions,
    ) -> Result<Plan> {
        if !columns.is_empty() && !self.inner.capability().supports_blob_handle {
            return Err(Error::Unsupported("blob URL inputs".into()));
        }
        options.download_recovery = Some(DownloadRecovery {
            columns: columns.clone(),
            policy: self.failure_policy,
            metrics: self.metrics.clone(),
        });
        let mut remaps = Vec::with_capacity(columns.len());
        for name in columns.iter() {
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
                field
                    .clone()
                    .with_nullable(
                        field.is_nullable() || self.failure_policy == DownloadFailurePolicy::Null,
                    )
                    .with_metadata(metadata),
                Ok,
            ));
        }
        let plan = Plan::new(schema, &remaps)?;
        // Keep the caller's input schema on the outer writer. The metadata-only
        // transform shares arrays, so it cannot reserve a payload twice.
        *schema = plan.target.clone();
        Ok(plan)
    }
}
