//! Select URL leaves for the backend's concurrency-limited blob downloader.
//!
//! Opening a writer resolves selections, marks only their text leaves, and adapts
//! nested array metadata. Values remain URLs here; the backend converts the marked
//! leaves to logical Blobs and owns all downloading and concurrency control.
use std::{collections::HashSet, sync::Arc};

use arrow_cast::cast;
use arrow_schema::{DataType, Field, FieldRef, Schema};
use open_arrow_sink_core::{
    BLOB_METADATA_KEY, BLOB_METADATA_VALUE, DownloadFailurePolicy, DownloadMetrics,
    DownloadRecovery, Error, Layer, Result, SchemaRef, Sink, SinkCapability, WriteOptions,
    WriteSession, Writer, validation,
};

use crate::{
    decorate::{Decorator, impl_sink},
    schema_evolution::{FieldRemap, Plan},
};

/// Interpret selected text fields as blob URLs, including fields nested in structs and lists.
/// Paths use dots between struct fields and implicitly traverse list elements.
/// An exact top-level name takes precedence over path traversal.
/// With the `Null` failure policy, a failed nested download nulls its top-level column.
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

    fn prepare(&self, schema: &mut SchemaRef, options: &mut WriteOptions) -> Result<Plan> {
        if !self.columns.is_empty() && !self.inner.capability().supports_blob_handle {
            return Err(Error::Unsupported("blob URL inputs".into()));
        }
        let (fields, roots) = mark_selected_urls(schema, &self.columns, self.failure_policy)?;
        options.download_recovery = Some(DownloadRecovery {
            columns: roots.clone(),
            policy: self.failure_policy,
            metrics: self.metrics.clone(),
        });
        let remaps = metadata_adapters(fields, &roots);
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

/// Resolve every selection against the original schema, but accumulate markings so
/// paths such as `parent.image` and `parent.thumbnail` can share a root.
fn mark_selected_urls(
    schema: &Schema,
    columns: &HashSet<String>,
    failure_policy: DownloadFailurePolicy,
) -> Result<(Vec<FieldRef>, HashSet<String>)> {
    let mut fields = schema.fields().to_vec();
    let mut roots = HashSet::new();
    for name in columns.iter() {
        // A literal top-level name wins even when it contains dots.
        let path = if validation::unique_column_index(schema, name).is_some() {
            vec![name.clone()]
        } else {
            name.split('.').map(str::to_owned).collect()
        };
        let selected = validation::unique_column_index(schema, &path[0]).and_then(|index| {
            mark_blob_field(&fields[index], &path[1..]).map(|field| (index, field))
        });
        let (index, field) = selected.ok_or_else(|| {
            Error::InvalidInput(format!(
                "blob URL path must select a unique text field: {name}"
            ))
        })?;
        // Recovery replaces the whole top-level value, including sibling fields.
        fields[index] = Arc::new(field.with_nullable(
            fields[index].is_nullable() || failure_policy == DownloadFailurePolicy::Null,
        ));
        roots.insert(path[0].clone());
    }
    Ok((fields, roots))
}

/// Nested fields are part of an array's DataType. Updating the batch schema alone
/// would leave those fields unmarked, so adapt the arrays too, sharing buffers.
fn metadata_adapters(fields: Vec<FieldRef>, roots: &HashSet<String>) -> Vec<FieldRemap> {
    fields
        .into_iter()
        .filter(|field| roots.contains(field.name()))
        .map(|field| {
            let ty = field.data_type().clone();
            FieldRemap::new(field.name().clone(), field, move |array| {
                cast(&array, &ty).map_err(Error::backend)
            })
        })
        .collect()
}

/// Consume one path segment per struct field. List elements are implicit and do
/// not consume a segment: `parent.items` can select a List<Utf8> leaf.
fn mark_blob_field(field: &Field, path: &[String]) -> Option<Field> {
    let ty = match field.data_type() {
        DataType::List(item) => DataType::List(Arc::new(mark_blob_field(item, path)?)),
        DataType::LargeList(item) => DataType::LargeList(Arc::new(mark_blob_field(item, path)?)),
        DataType::FixedSizeList(item, size) => {
            DataType::FixedSizeList(Arc::new(mark_blob_field(item, path)?), *size)
        }
        DataType::Struct(fields) if !path.is_empty() => {
            let index = validation::unique_field_index(fields, &path[0])?;
            let mut fields = fields.to_vec();
            fields[index] = Arc::new(mark_blob_field(&fields[index], &path[1..])?);
            DataType::Struct(fields.into())
        }
        ty if path.is_empty() && ty.is_string() => {
            let mut metadata = field.metadata().clone();
            metadata.insert(BLOB_METADATA_KEY.into(), BLOB_METADATA_VALUE.into());
            return Some(field.clone().with_metadata(metadata));
        }
        _ => return None,
    };
    Some(field.clone().with_data_type(ty))
}
