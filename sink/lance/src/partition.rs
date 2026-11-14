//! Bounded, create-only partition routing for local Lance datasets.
use std::{
    collections::{BTreeMap, HashSet},
    fmt,
    num::NonZeroUsize,
    path::PathBuf,
};

use arrow_array::{
    Array, BooleanArray, Int8Array, Int16Array, Int32Array, Int64Array, LargeStringArray,
    StringArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
use arrow_schema::DataType;
use arrow_select::take::take_record_batch;
use open_arrow_sink_core::{
    Error, Layer, Ordering, RecordBatch, Result, SchemaRef, Sink, SinkCapability, WriteOptions,
    WriteSummary, Writer, async_trait, validation,
};

use crate::{IndexBuildReport, IndexCommitError, LanceSink, error::backend};

/// Routes rows by named string, integer, or boolean columns. Partition columns
/// remain in the stored schema. Limits are hard errors, never early commits.
#[derive(Debug, Clone)]
pub struct LancePartitionLayer {
    columns: Vec<String>,
    max_partitions: NonZeroUsize,
    max_batch_bytes: NonZeroUsize,
}

impl LancePartitionLayer {
    pub fn new(columns: Vec<String>, max_partitions: NonZeroUsize) -> Self {
        Self {
            columns,
            max_partitions,
            max_batch_bytes: NonZeroUsize::new(64 * 1024 * 1024).unwrap(),
        }
    }
    /// Includes the input batch's retained Arrow buffers, even for slices.
    pub fn with_max_batch_bytes(mut self, limit: NonZeroUsize) -> Self {
        self.max_batch_bytes = limit;
        self
    }
}

#[derive(Debug, Clone)]
pub struct PartitionedLanceSink<S> {
    inner: S,
    config: LancePartitionLayer,
}

impl<S: LanceSink> Layer<S> for LancePartitionLayer {
    type Layered = PartitionedLanceSink<S>;
    fn layer(&self, inner: S) -> Self::Layered {
        PartitionedLanceSink {
            inner,
            config: self.clone(),
        }
    }
}

#[async_trait]
impl<S: LanceSink> Sink for PartitionedLanceSink<S> {
    type Request = PathBuf;
    fn capability(&self) -> SinkCapability {
        SinkCapability {
            ordering: Ordering::Unordered,
        }
    }
    async fn open(
        &self,
        path: PathBuf,
        schema: SchemaRef,
        options: WriteOptions,
    ) -> Result<Box<dyn Writer>> {
        validation::validate_options(&options, self.capability())?;
        self.inner.validate_schema(&schema)?;
        if self.config.columns.is_empty() {
            return Err(Error::Unsupported(
                "at least one partition column is required".into(),
            ));
        }
        let mut seen = HashSet::new();
        let mut columns = Vec::new();
        for name in &self.config.columns {
            if !seen.insert(name) {
                return Err(Error::Unsupported(format!(
                    "duplicate partition column: {name}"
                )));
            }
            let position = schema
                .index_of(name)
                .map_err(|_| Error::Unsupported(format!("partition column not found: {name}")))?;
            if !matches!(
                schema.field(position).data_type(),
                DataType::Utf8
                    | DataType::LargeUtf8
                    | DataType::Boolean
                    | DataType::Int8
                    | DataType::Int16
                    | DataType::Int32
                    | DataType::Int64
                    | DataType::UInt8
                    | DataType::UInt16
                    | DataType::UInt32
                    | DataType::UInt64
            ) {
                return Err(Error::Unsupported(format!(
                    "unsupported partition column type: {name}"
                )));
            }
            columns.push(position);
        }
        let path = std::path::absolute(path).map_err(backend)?;
        if path.to_str().is_none() {
            return Err(Error::Unsupported("Lance requires a UTF-8 path".into()));
        }
        tokio::fs::create_dir(&path).await.map_err(backend)?;
        Ok(Box::new(PartitionWriter {
            inner: self.inner.clone(),
            schema,
            path,
            columns,
            config: self.config.clone(),
            options,
            partitions: BTreeMap::new(),
            failed: false,
        }))
    }
}

/// Values are rendered within the session's fixed schema. Null and empty string
/// are distinct. Column order is the configured partition-column order.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct PartitionKey {
    pub values: Vec<Option<String>>,
}

impl PartitionKey {
    pub fn directory_name(&self) -> String {
        self.values
            .iter()
            .enumerate()
            .map(|(i, value)| match value {
                None => format!("p{i}_n"),
                Some(value) => format!("p{i}_v{}", crate::hex(value.as_bytes())),
            })
            .collect::<Vec<_>>()
            .join("__")
    }
}

struct PartitionWriter<S> {
    inner: S,
    schema: SchemaRef,
    path: PathBuf,
    columns: Vec<usize>,
    config: LancePartitionLayer,
    options: WriteOptions,
    partitions: BTreeMap<PartitionKey, Box<dyn Writer>>,
    failed: bool,
}

#[derive(Debug, Clone)]
pub enum PartitionCommitState {
    Complete {
        rows_written: u64,
    },
    /// No atomic rollback is promised. A native commit error can have an
    /// unknown publication outcome; an index report confirms data publication.
    Failed {
        message: String,
        index_report: Option<IndexBuildReport>,
    },
}

#[derive(Debug, Clone)]
pub struct PartitionCommitResult {
    pub key: PartitionKey,
    pub path: PathBuf,
    pub state: PartitionCommitState,
}

/// Returned inside `Error::Backend` when one or more partition commits fail.
/// Every partition is attempted; successful partitions remain visible.
#[derive(Debug)]
pub struct PartitionCommitError {
    pub partitions: Vec<PartitionCommitResult>,
}
impl fmt::Display for PartitionCommitError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let failed = self
            .partitions
            .iter()
            .filter(|p| matches!(p.state, PartitionCommitState::Failed { .. }))
            .count();
        write!(
            f,
            "{failed} of {} Lance partitions failed to commit or index; inspect partition results before retrying",
            self.partitions.len()
        )
    }
}
impl std::error::Error for PartitionCommitError {}

#[async_trait]
impl<S: LanceSink> Writer for PartitionWriter<S> {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }
    async fn write(&mut self, batch: RecordBatch) -> Result<()> {
        if self.failed {
            return Err(Error::Unsupported(
                "partition writer failed; abort or drop it".into(),
            ));
        }
        // Cancellation or any error makes this session terminal.
        self.failed = true;
        validation::validate_batch(&self.schema, &batch)?;
        if batch.get_array_memory_size() > self.config.max_batch_bytes.get() {
            return Err(Error::Unsupported(
                "partition batch exceeds max_batch_bytes".into(),
            ));
        }
        let mut groups: BTreeMap<PartitionKey, Vec<u64>> = BTreeMap::new();
        for row in 0..batch.num_rows() {
            let key = PartitionKey {
                values: self
                    .columns
                    .iter()
                    .map(|&col| value(batch.column(col).as_ref(), row))
                    .collect(),
            };
            // Keep a path component below common local filesystem limits.
            if key.directory_name().len() > 200 {
                return Err(Error::Unsupported(
                    "encoded partition key exceeds 200 bytes".into(),
                ));
            }
            groups.entry(key).or_default().push(row as u64);
        }
        let additional = groups
            .keys()
            .filter(|key| !self.partitions.contains_key(*key))
            .count();
        if self.partitions.len() + additional > self.config.max_partitions.get() {
            return Err(Error::Unsupported(
                "partition count exceeds max_partitions".into(),
            ));
        }
        for (key, rows) in groups {
            let selected = take_record_batch(&batch, &UInt64Array::from(rows)).map_err(backend)?;
            if !self.partitions.contains_key(&key) {
                let writer = self
                    .inner
                    .open(
                        self.path.join(key.directory_name()),
                        self.schema.clone(),
                        self.options,
                    )
                    .await?;
                self.partitions.insert(key.clone(), writer);
            }
            self.partitions
                .get_mut(&key)
                .unwrap()
                .write(selected)
                .await?;
        }
        self.failed = false;
        Ok(())
    }

    async fn commit(mut self: Box<Self>) -> Result<WriteSummary> {
        if self.failed {
            self.abort().await?;
            return Err(Error::Unsupported(
                "partition writer failed; no partitions committed".into(),
            ));
        }
        let mut results = Vec::new();
        let mut summary = WriteSummary::default();
        let mut failed = false;
        for (key, writer) in std::mem::take(&mut self.partitions) {
            let state = match writer.commit().await {
                Ok(result) => {
                    summary.rows_written += result.rows_written;
                    PartitionCommitState::Complete {
                        rows_written: result.rows_written,
                    }
                }
                Err(error) => {
                    failed = true;
                    let index_report = match &error {
                        Error::Backend(source) => source
                            .downcast_ref::<IndexCommitError>()
                            .map(|e| e.report.clone()),
                        _ => None,
                    };
                    PartitionCommitState::Failed {
                        message: error.to_string(),
                        index_report,
                    }
                }
            };
            results.push(PartitionCommitResult {
                path: self.path.join(key.directory_name()),
                key,
                state,
            });
        }
        if failed {
            Err(backend(PartitionCommitError {
                partitions: results,
            }))
        } else {
            Ok(summary)
        }
    }

    async fn abort(mut self: Box<Self>) -> Result<()> {
        let mut first_error = None;
        for (_, writer) in std::mem::take(&mut self.partitions) {
            if let Err(error) = writer.abort().await {
                first_error.get_or_insert(error);
            }
        }
        if self.options.cleanup_on_error
            && let Err(error) = tokio::fs::remove_dir_all(&self.path).await
        {
            first_error.get_or_insert_with(|| backend(error));
        }
        match first_error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }
}

fn value(array: &dyn Array, row: usize) -> Option<String> {
    if array.is_null(row) {
        return None;
    }
    macro_rules! primitive {
        ($ty:ty) => {
            array
                .as_any()
                .downcast_ref::<$ty>()
                .unwrap()
                .value(row)
                .to_string()
        };
    }
    Some(match array.data_type() {
        DataType::Utf8 => primitive!(StringArray),
        DataType::LargeUtf8 => primitive!(LargeStringArray),
        DataType::Boolean => primitive!(BooleanArray),
        DataType::Int8 => primitive!(Int8Array),
        DataType::Int16 => primitive!(Int16Array),
        DataType::Int32 => primitive!(Int32Array),
        DataType::Int64 => primitive!(Int64Array),
        DataType::UInt8 => primitive!(UInt8Array),
        DataType::UInt16 => primitive!(UInt16Array),
        DataType::UInt32 => primitive!(UInt32Array),
        DataType::UInt64 => primitive!(UInt64Array),
        _ => unreachable!("partition types validated at open"),
    })
}
