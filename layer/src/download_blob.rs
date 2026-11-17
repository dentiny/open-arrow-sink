//! Explicitly select URL columns for the backend's bounded blob downloader.
use std::{collections::HashSet, sync::Arc};

use arrow_array::{ArrayRef, make_array};
use arrow_schema::{DataType, Field};
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
        let mut selected = HashSet::new();
        let mut roots = HashSet::new();
        for name in self.columns.iter() {
            let path = if validation::unique_column_index(schema, name).is_some() {
                vec![name.clone()]
            } else {
                name.split('.').map(str::to_owned).collect()
            };
            let field = validation::nested_field(schema, &path).ok_or_else(|| {
                Error::InvalidInput(format!("blob URL field must exist and be unique: {name}"))
            })?;
            let mut ty = field.data_type();
            while let DataType::List(item)
            | DataType::LargeList(item)
            | DataType::FixedSizeList(item, _) = ty
            {
                ty = item.data_type();
            }
            if !ty.is_string() {
                return Err(Error::InvalidInput(format!(
                    "blob URL field must be text: {name}"
                )));
            }
            roots.insert(path[0].clone());
            selected.insert(path);
        }
        options.download_recovery = Some(DownloadRecovery {
            columns: roots.clone(),
            policy: self.failure_policy,
            metrics: self.metrics.clone(),
        });
        let remaps = schema
            .fields()
            .iter()
            .filter(|field| roots.contains(field.name()))
            .map(|field| {
                let target = mark_blob_fields(field, &[field.name().clone()], &selected)
                    .with_nullable(
                        field.is_nullable() || self.failure_policy == DownloadFailurePolicy::Null,
                    );
                let ty = target.data_type().clone();
                FieldRemap::new(field.name().clone(), target, move |array| {
                    retype_array(array, &ty)
                })
            })
            .collect::<Vec<_>>();
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

fn mark_blob_fields(field: &Field, path: &[String], selected: &HashSet<Vec<String>>) -> Field {
    let ty = match field.data_type() {
        DataType::List(item) => DataType::List(Arc::new(mark_blob_fields(item, path, selected))),
        DataType::LargeList(item) => {
            DataType::LargeList(Arc::new(mark_blob_fields(item, path, selected)))
        }
        DataType::FixedSizeList(item, size) => {
            DataType::FixedSizeList(Arc::new(mark_blob_fields(item, path, selected)), *size)
        }
        _ if selected.contains(path) => {
            let mut metadata = field.metadata().clone();
            metadata.insert(BLOB_METADATA_KEY.into(), BLOB_METADATA_VALUE.into());
            return field.clone().with_metadata(metadata);
        }
        DataType::Struct(fields) => DataType::Struct(
            fields
                .iter()
                .map(|child| {
                    let mut path = path.to_vec();
                    path.push(child.name().clone());
                    Arc::new(mark_blob_fields(child, &path, selected))
                })
                .collect(),
        ),
        _ => return field.clone(),
    };
    field.clone().with_data_type(ty)
}

fn retype_array(array: ArrayRef, ty: &DataType) -> Result<ArrayRef> {
    if array.data_type() == ty {
        return Ok(array);
    }
    let fields: &[Arc<Field>] = match ty {
        DataType::Struct(fields) => fields,
        DataType::List(item) | DataType::LargeList(item) | DataType::FixedSizeList(item, _) => {
            std::slice::from_ref(item)
        }
        _ => unreachable!("only container metadata changes"),
    };
    let data = array.to_data();
    let children = fields
        .iter()
        .zip(data.child_data())
        .map(|(field, child)| {
            retype_array(make_array(child.clone()), field.data_type()).map(|array| array.to_data())
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(make_array(
        data.into_builder()
            .data_type(ty.clone())
            .child_data(children)
            .build()
            .map_err(Error::backend)?,
    ))
}
