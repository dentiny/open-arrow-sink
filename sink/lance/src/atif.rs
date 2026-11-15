//! Optional ATIF field-metadata adaptation; no document parsing or media fetching.
use crate::error::backend;
use arrow_array::{Array, ArrayRef, RecordBatchOptions, StructArray, make_array, new_null_array};
use arrow_schema::{DataType, FieldRef, Schema};
use open_arrow_sink_core::{
    BlobHandleConfig, DataWriter, Error, Layer, ParallelSink, RecordBatch, Result, SchemaRef, Sink,
    SinkCapability, WriteOptions, WriteSession, WriteSummary, Writer, async_trait, validation,
};
use std::{num::NonZeroUsize, sync::Arc};

const SOURCE: &str = "lance_conversion:source_type";
const POLICY: &str = "lance_conversion:storage_policy";

/// Convert explicitly marked ATIF binary fields to text or native logical blobs.
/// Compose with LanceDatasetSink; unmarked fields retain their original storage.
#[derive(Debug, Clone, Copy, Default)]
pub struct AtifStorageLayer;

pub struct AtifStorageSink<S> {
    inner: S,
}
impl<S: Sink> Layer<S> for AtifStorageLayer {
    type Layered = AtifStorageSink<S>;
    fn layer(&self, inner: S) -> Self::Layered {
        AtifStorageSink { inner }
    }
}

fn binary(ty: &DataType) -> bool {
    matches!(
        ty,
        DataType::Binary | DataType::LargeBinary | DataType::BinaryView
    )
}
fn blob(field: &arrow_schema::Field) -> bool {
    field
        .metadata()
        .get("ARROW:extension:name")
        .map(String::as_str)
        == Some("lance.blob.v2")
}
fn field_schema(field: &FieldRef, config: Option<&BlobHandleConfig>) -> Result<FieldRef> {
    if blob(field) {
        return Ok(field.clone());
    }
    if field.metadata().get(SOURCE).map(String::as_str) == Some("atif")
        && let Some(policy) = field.metadata().get(POLICY)
    {
        if !binary(field.data_type()) {
            return Err(Error::Unsupported(format!(
                "ATIF storage policy requires binary field {}",
                field.name()
            )));
        }
        let output = match policy.as_str() {
            "text" => {
                let mut field = field.as_ref().clone().with_data_type(DataType::LargeBinary);
                field
                    .metadata_mut()
                    .entry("lance-encoding:compression".into())
                    .or_insert_with(|| "zstd".into());
                field
            }
            "blob" => {
                let native = lance::blob::blob_field_with_options(
                    field.name(),
                    field.is_nullable(),
                    lance::blob::BlobFieldOptions {
                        inline_size_threshold: config.and_then(|c| c.inline_size_threshold),
                        dedicated_size_threshold: config.and_then(|c| c.dedicated_size_threshold),
                    },
                );
                let mut metadata = field.metadata().clone();
                metadata.extend(native.metadata().clone());
                native.with_metadata(metadata)
            }
            _ => {
                return Err(Error::Unsupported(format!(
                    "invalid ATIF storage policy: {policy}"
                )));
            }
        };
        return Ok(Arc::new(output));
    }
    let ty = match field.data_type() {
        DataType::Struct(children) => DataType::Struct(
            children
                .iter()
                .map(|f| field_schema(f, config))
                .collect::<Result<_>>()?,
        ),
        DataType::List(item) => DataType::List(field_schema(item, config)?),
        DataType::LargeList(item) => DataType::LargeList(field_schema(item, config)?),
        DataType::FixedSizeList(item, size) => {
            DataType::FixedSizeList(field_schema(item, config)?, *size)
        }
        _ => return Ok(field.clone()),
    };
    Ok(Arc::new(field.as_ref().clone().with_data_type(ty)))
}
fn output_schema(
    schema: &SchemaRef,
    options: &WriteOptions,
    capability: &SinkCapability,
) -> Result<SchemaRef> {
    let fields = schema
        .fields()
        .iter()
        .map(|f| field_schema(f, options.blob_handle.as_ref()))
        .collect::<Result<Vec<_>>>()?;
    let output = Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone()));
    if output != *schema && !capability.supports_blob_handle {
        return Err(Error::Unsupported(
            "ATIF storage adaptation requires native blob support".into(),
        ));
    }
    Ok(output)
}
fn transform(array: &ArrayRef, field: &arrow_schema::Field) -> Result<ArrayRef> {
    if array.data_type() == field.data_type() {
        return Ok(array.clone());
    }
    if blob(field) && binary(array.data_type()) {
        let DataType::Struct(children) = field.data_type() else {
            unreachable!("native blob schema")
        };
        let data = arrow_cast::cast(array, &DataType::LargeBinary).map_err(backend)?;
        let uri = new_null_array(&DataType::Utf8, array.len());
        return Ok(Arc::new(
            StructArray::try_new(children.clone(), vec![data, uri], array.nulls().cloned())
                .map_err(backend)?,
        ));
    }
    if binary(array.data_type()) && field.data_type() == &DataType::LargeBinary {
        return arrow_cast::cast(array, field.data_type()).map_err(backend);
    }
    let children: &[FieldRef] = match field.data_type() {
        DataType::Struct(fields) => fields,
        DataType::List(item) | DataType::LargeList(item) | DataType::FixedSizeList(item, _) => {
            std::slice::from_ref(item)
        }
        _ => {
            return Err(Error::Unsupported(format!(
                "ATIF container {}",
                field.data_type()
            )));
        }
    };
    let data = array.to_data();
    let transformed = data
        .child_data()
        .iter()
        .zip(children)
        .map(|(data, field)| transform(&make_array(data.clone()), field).map(|a| a.to_data()))
        .collect::<Result<Vec<_>>>()?;
    Ok(make_array(
        data.into_builder()
            .data_type(field.data_type().clone())
            .child_data(transformed)
            .build()
            .map_err(backend)?,
    ))
}
fn batch(input: RecordBatch, schema: &SchemaRef) -> Result<RecordBatch> {
    let columns = input
        .columns()
        .iter()
        .zip(schema.fields())
        .map(|(a, f)| transform(a, f))
        .collect::<Result<_>>()?;
    RecordBatch::try_new_with_options(
        schema.clone(),
        columns,
        &RecordBatchOptions::new().with_row_count(Some(input.num_rows())),
    )
    .map_err(backend)
}
fn state(failed: bool) -> Result<()> {
    if failed {
        Err(Error::Unsupported(
            "ATIF writer failed; abort or drop it".into(),
        ))
    } else {
        Ok(())
    }
}
#[async_trait]
impl<S: Sink> Sink for AtifStorageSink<S> {
    type Request = S::Request;
    fn capability(&self) -> SinkCapability {
        SinkCapability {
            supports_blob_streaming: false,
            ..self.inner.capability()
        }
    }
    async fn open(
        &self,
        request: Self::Request,
        schema: SchemaRef,
        options: WriteOptions,
    ) -> Result<Box<dyn Writer>> {
        let output = output_schema(&schema, &options, &self.inner.capability())?;
        let inner = self.inner.open(request, output.clone(), options).await?;
        Ok(Box::new(AtifWriter {
            inner,
            schema,
            output,
            failed: false,
        }))
    }
}
struct AtifWriter {
    inner: Box<dyn Writer>,
    schema: SchemaRef,
    output: SchemaRef,
    failed: bool,
}
#[async_trait]
impl Writer for AtifWriter {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }
    async fn write(&mut self, input: RecordBatch) -> Result<()> {
        state(self.failed)?;
        self.failed = true;
        validation::validate_batch(&self.schema, &input)?;
        self.inner.write(batch(input, &self.output)?).await?;
        self.failed = false;
        Ok(())
    }
    async fn commit(self: Box<Self>) -> Result<WriteSummary> {
        if let Err(error) = state(self.failed) {
            let _ = self.inner.abort().await;
            return Err(error);
        }
        self.inner.commit().await
    }
    async fn abort(self: Box<Self>) -> Result<()> {
        self.inner.abort().await
    }
}
struct AtifDataWriter<T: Send + 'static> {
    inner: Box<dyn DataWriter<Output = T>>,
    schema: SchemaRef,
    output: SchemaRef,
    failed: bool,
}
#[async_trait]
impl<T: Send + 'static> DataWriter for AtifDataWriter<T> {
    type Output = T;
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }
    async fn write(&mut self, input: RecordBatch) -> Result<()> {
        state(self.failed)?;
        self.failed = true;
        validation::validate_batch(&self.schema, &input)?;
        self.inner.write(batch(input, &self.output)?).await?;
        self.failed = false;
        Ok(())
    }
    async fn finish(&mut self) -> Result<T> {
        state(self.failed)?;
        self.failed = true;
        self.inner.finish().await
    }
    async fn abort(self: Box<Self>) -> Result<()> {
        self.inner.abort().await
    }
}
#[async_trait]
impl<S: ParallelSink> ParallelSink for AtifStorageSink<S> {
    type Output = S::Output;
    async fn open_writers(
        &self,
        request: Self::Request,
        schema: SchemaRef,
        options: WriteOptions,
        count: NonZeroUsize,
    ) -> Result<WriteSession<Self::Output>> {
        let output = output_schema(&schema, &options, &self.inner.capability())?;
        let session = self
            .inner
            .open_writers(request, output.clone(), options, count)
            .await?;
        Ok(WriteSession {
            writers: session
                .writers
                .into_iter()
                .map(|inner| {
                    Box::new(AtifDataWriter {
                        inner,
                        schema: schema.clone(),
                        output: output.clone(),
                        failed: false,
                    }) as Box<dyn DataWriter<Output = Self::Output>>
                })
                .collect(),
            committer: session.committer,
        })
    }
}
