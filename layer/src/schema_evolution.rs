//! One-to-one top-level field remapping with a fixed schema per session.
use arrow_array::{ArrayRef, RecordBatchOptions};
use arrow_schema::{FieldRef, Schema};
use open_arrow_sink_core::{
    DataWriter, Error, GuardedDataWriter, GuardedWriter, Layer, ParallelSink, RecordBatch, Result,
    SchemaRef, Sink, SinkCapability, WriteOptions, WriteSession, WriteSummary, Writer, async_trait,
    validation,
};
use std::{collections::HashSet, num::NonZeroUsize, sync::Arc};

type Transform = dyn Fn(ArrayRef) -> Result<ArrayRef> + Send + Sync;

/// Replace one top-level field and its array, retaining its column position.
/// The callback must preserve row count and row order, including nested offsets
/// and nulls when applicable. Output type and nullability are checked per batch.
#[derive(Clone)]
pub struct FieldRemap {
    source: String,
    target: FieldRef,
    transform: Arc<Transform>,
}
impl FieldRemap {
    pub fn new(
        source: impl Into<String>,
        target: impl Into<FieldRef>,
        transform: impl Fn(ArrayRef) -> Result<ArrayRef> + Send + Sync + 'static,
    ) -> Self {
        Self {
            source: source.into(),
            target: target.into(),
            transform: Arc::new(transform),
        }
    }
}

/// Remap fields without adding, removing, or reordering columns.
/// All source names resolve against the original input schema. Duplicate source
/// mappings and duplicate output names fail before opening the destination.
/// Unmapped fields and schema metadata are retained. Callers provide any format-
/// specific policy; this layer does not interpret ATIF or Lance metadata.
#[derive(Clone, Default)]
pub struct SchemaEvolutionLayer {
    remaps: Arc<[FieldRemap]>,
}
impl SchemaEvolutionLayer {
    pub fn new(remaps: Vec<FieldRemap>) -> Self {
        Self {
            remaps: remaps.into(),
        }
    }
}
pub struct SchemaEvolutionSink<S> {
    inner: S,
    remaps: Arc<[FieldRemap]>,
}
impl<S: Sink> Layer<S> for SchemaEvolutionLayer {
    type Layered = SchemaEvolutionSink<S>;
    fn layer(&self, inner: S) -> Self::Layered {
        SchemaEvolutionSink {
            inner,
            remaps: self.remaps.clone(),
        }
    }
}
#[derive(Clone)]
struct Plan {
    source: SchemaRef,
    target: SchemaRef,
    steps: Vec<(usize, FieldRemap)>,
}
impl Plan {
    fn new(source: &SchemaRef, remaps: &[FieldRemap]) -> Result<Self> {
        let mut fields = source.fields().to_vec();
        let mut seen = HashSet::new();
        let mut steps = Vec::new();
        for remap in remaps {
            let matches: Vec<_> = source
                .fields()
                .iter()
                .enumerate()
                .filter(|(_, f)| f.name() == &remap.source)
                .map(|(i, _)| i)
                .collect();
            if matches.len() != 1 || !seen.insert(remap.source.clone()) {
                return Err(Error::Unsupported(format!(
                    "remap source must exist and be unique: {}",
                    remap.source
                )));
            }
            let index = matches[0];
            fields[index] = remap.target.clone();
            steps.push((index, remap.clone()));
        }
        let mut names = HashSet::new();
        if fields.iter().any(|f| !names.insert(f.name())) {
            return Err(Error::Unsupported("duplicate remapped field name".into()));
        }
        Ok(Self {
            source: source.clone(),
            target: Arc::new(Schema::new_with_metadata(fields, source.metadata().clone())),
            steps,
        })
    }
    fn apply(&self, input: RecordBatch) -> Result<RecordBatch> {
        validation::validate_batch(&self.source, &input)?;
        let mut columns = input.columns().to_vec();
        for (index, remap) in &self.steps {
            let array = (remap.transform)(columns[*index].clone())?;
            if array.len() != input.num_rows()
                || array.data_type() != remap.target.data_type()
                || (!remap.target.is_nullable() && array.logical_null_count() != 0)
            {
                return Err(Error::SchemaMismatch);
            }
            columns[*index] = array;
        }
        RecordBatch::try_new_with_options(
            self.target.clone(),
            columns,
            &RecordBatchOptions::new().with_row_count(Some(input.num_rows())),
        )
        .map_err(Error::backend)
    }
}
#[async_trait]
impl<S: Sink> Sink for SchemaEvolutionSink<S> {
    type Request = S::Request;
    fn capability(&self) -> SinkCapability {
        self.inner.capability().with_supports_blob_streaming(false)
    }
    async fn open(
        &self,
        request: Self::Request,
        schema: SchemaRef,
        options: WriteOptions,
    ) -> Result<Box<dyn Writer>> {
        let output = Plan::new(&schema, &self.remaps)?;
        let inner = self
            .inner
            .open(request, output.target.clone(), options)
            .await?;
        Ok(Box::new(GuardedWriter::new(Box::new(EvolutionWriter {
            inner,
            schema,
            output,
        }))))
    }
}
struct EvolutionWriter {
    inner: Box<dyn Writer>,
    schema: SchemaRef,
    output: Plan,
}
#[async_trait]
impl Writer for EvolutionWriter {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }
    async fn write(&mut self, input: RecordBatch) -> Result<()> {
        self.inner.write(self.output.apply(input)?).await?;
        Ok(())
    }
    async fn commit(self: Box<Self>) -> Result<WriteSummary> {
        self.inner.commit().await
    }
    async fn abort(self: Box<Self>) -> Result<()> {
        self.inner.abort().await
    }
}
struct EvolutionDataWriter<T: Send + 'static> {
    inner: Box<dyn DataWriter<Output = T>>,
    schema: SchemaRef,
    output: Plan,
}
#[async_trait]
impl<T: Send + 'static> DataWriter for EvolutionDataWriter<T> {
    type Output = T;
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }
    async fn write(&mut self, input: RecordBatch) -> Result<()> {
        self.inner.write(self.output.apply(input)?).await?;
        Ok(())
    }
    async fn finish(&mut self) -> Result<T> {
        self.inner.finish().await
    }
    async fn abort(self: Box<Self>) -> Result<()> {
        self.inner.abort().await
    }
}
#[async_trait]
impl<S: ParallelSink> ParallelSink for SchemaEvolutionSink<S> {
    type Output = S::Output;
    async fn open_writers(
        &self,
        request: Self::Request,
        schema: SchemaRef,
        options: WriteOptions,
        count: NonZeroUsize,
    ) -> Result<WriteSession<Self::Output>> {
        let output = Plan::new(&schema, &self.remaps)?;
        let session = self
            .inner
            .open_writers(request, output.target.clone(), options, count)
            .await?;
        Ok(WriteSession {
            writers: session
                .writers
                .into_iter()
                .map(|inner| {
                    Box::new(GuardedDataWriter::new(Box::new(EvolutionDataWriter {
                        inner,
                        schema: schema.clone(),
                        output: output.clone(),
                    }))) as Box<dyn DataWriter<Output = Self::Output>>
                })
                .collect(),
            committer: session.committer,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int32Array, Int64Array};
    use arrow_schema::{DataType, Field};

    fn input() -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new_with_metadata(
                vec![
                    Field::new("a", DataType::Int32, true),
                    Field::new("b", DataType::Int32, false),
                ],
                [("custom".into(), "kept".into())].into(),
            )),
            vec![
                Arc::new(Int32Array::from(vec![Some(1), None, Some(3)])),
                Arc::new(Int32Array::from(vec![4, 5, 6])),
            ],
        )
        .unwrap()
        .slice(1, 2)
    }

    #[test]
    fn remaps_sliced_arrays_and_preserves_other_columns() {
        let input = input();
        let target = Field::new("renamed", DataType::Int64, true)
            .with_metadata([("policy".into(), "custom".into())].into());
        let plan = Plan::new(
            &input.schema(),
            &[FieldRemap::new("a", target.clone(), |array| {
                let array = array.as_any().downcast_ref::<Int32Array>().unwrap();
                Ok(Arc::new(Int64Array::from_iter(
                    array.iter().map(|v| v.map(i64::from)),
                )))
            })],
        )
        .unwrap();
        let output = plan.apply(input.clone()).unwrap();
        assert_eq!(output.num_rows(), 2);
        assert_eq!(output.schema().field(0), &target);
        assert_eq!(output.schema().metadata(), input.schema().metadata());
        assert!(Arc::ptr_eq(output.column(1), input.column(1)));
        assert_eq!(
            output
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap(),
            &Int64Array::from(vec![None, Some(3)])
        );
    }

    #[test]
    fn validates_names_and_allows_simultaneous_swaps() {
        let schema = input().schema();
        let remap = |source: &str, target: &str| {
            FieldRemap::new(source, Field::new(target, DataType::Int32, true), Ok)
        };
        assert!(Plan::new(&schema, &[remap("missing", "c")]).is_err());
        assert!(Plan::new(&schema, &[remap("a", "c"), remap("a", "d")]).is_err());
        assert!(Plan::new(&schema, &[remap("a", "b")]).is_err());
        let plan = Plan::new(&schema, &[remap("a", "b"), remap("b", "a")]).unwrap();
        assert_eq!(plan.target.field(0).name(), "b");
        assert_eq!(plan.target.field(1).name(), "a");
    }

    #[test]
    fn rejects_wrong_type_length_and_nullability() {
        let input = input();
        for remap in [
            FieldRemap::new("a", Field::new("a", DataType::Int64, true), Ok),
            FieldRemap::new("a", Field::new("a", DataType::Int32, false), Ok),
            FieldRemap::new("a", Field::new("a", DataType::Int32, true), |a| {
                Ok(a.slice(0, 1))
            }),
        ] {
            let plan = Plan::new(&input.schema(), &[remap]).unwrap();
            assert!(plan.apply(input.clone()).is_err());
        }
    }

    #[test]
    fn preserves_empty_batch_row_count() {
        let schema = Arc::new(Schema::empty());
        let input = RecordBatch::try_new_with_options(
            schema.clone(),
            vec![],
            &RecordBatchOptions::new().with_row_count(Some(3)),
        )
        .unwrap();
        assert_eq!(
            Plan::new(&schema, &[])
                .unwrap()
                .apply(input)
                .unwrap()
                .num_rows(),
            3
        );
    }
}
