//! Lance-specific indexes built after the data commit.
use std::{fmt, path::PathBuf};

use lance::{
    Dataset,
    index::{DatasetIndexExt, vector::VectorIndexParams},
};
use lance_index::{
    IndexParams, IndexType,
    scalar::{BuiltinIndexType, InvertedIndexParams, ScalarIndexParams},
};
use lance_linalg::distance::MetricType;
use open_arrow_sink_core::{
    Layer, RecordBatch, Result, SchemaRef, Sink, SinkCapability, WriteOptions, WriteSummary,
    Writer, async_trait, validation,
};

use crate::{LanceDatasetSink, error::backend, validation::validate_indexes};

/// One index per column. Vector indexes use IVF_FLAT with L2 distance.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LanceIndexKind {
    Scalar,
    Vector,
    Text,
    BloomFilter,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LanceIndexSpec {
    pub column: String,
    pub kind: LanceIndexKind,
}

impl LanceIndexSpec {
    pub fn new(column: impl Into<String>, kind: LanceIndexKind) -> Self {
        Self {
            column: column.into(),
            kind,
        }
    }

    /// Hex encoding makes names deterministic and avoids column-name collisions.
    pub fn name(&self) -> String {
        let kind = match self.kind {
            LanceIndexKind::Scalar => "btree",
            LanceIndexKind::Vector => "ivf_flat",
            LanceIndexKind::Text => "text",
            LanceIndexKind::BloomFilter => "bloom",
        };
        format!("oas_{kind}_{}", hex(self.column.as_bytes()))
    }
}

/// Build requested indexes after the dataset's data commit.
#[derive(Debug, Clone)]
pub struct LanceIndexLayer {
    indexes: Vec<LanceIndexSpec>,
}

impl LanceIndexLayer {
    pub fn new(indexes: Vec<LanceIndexSpec>) -> Self {
        Self { indexes }
    }
}

#[derive(Debug, Clone)]
pub struct IndexedLanceSink {
    inner: LanceDatasetSink,
    indexes: Vec<LanceIndexSpec>,
}

impl Layer<LanceDatasetSink> for LanceIndexLayer {
    type Layered = IndexedLanceSink;
    fn layer(&self, inner: LanceDatasetSink) -> Self::Layered {
        IndexedLanceSink {
            inner,
            indexes: self.indexes.clone(),
        }
    }
}

#[async_trait]
impl Sink for IndexedLanceSink {
    type Request = PathBuf;
    fn capability(&self) -> SinkCapability {
        self.inner.capability()
    }
    async fn open(
        &self,
        path: PathBuf,
        schema: SchemaRef,
        options: WriteOptions,
    ) -> Result<Box<dyn Writer>> {
        validation::validate_options(&options, self.capability())?;
        validate_indexes(&schema, &self.indexes)?;
        let path = std::path::absolute(path).map_err(backend)?;
        let inner = self.inner.open(path.clone(), schema, options).await?;
        Ok(Box::new(IndexWriter {
            inner,
            path,
            indexes: self.indexes.clone(),
        }))
    }
}

struct IndexWriter {
    inner: Box<dyn Writer>,
    path: PathBuf,
    indexes: Vec<LanceIndexSpec>,
}

#[async_trait]
impl Writer for IndexWriter {
    fn schema(&self) -> &SchemaRef {
        self.inner.schema()
    }
    async fn write(&mut self, batch: RecordBatch) -> Result<()> {
        self.inner.write(batch).await
    }
    async fn abort(self: Box<Self>) -> Result<()> {
        self.inner.abort().await
    }
    async fn commit(self: Box<Self>) -> Result<WriteSummary> {
        let summary = self.inner.commit().await?;
        if self.indexes.is_empty() {
            return Ok(summary);
        }
        let mut report = IndexBuildReport {
            path: self.path,
            rows_written: summary.rows_written,
            data_version: None,
            latest_version: None,
            indexes: self
                .indexes
                .into_iter()
                .map(|spec| IndexBuildResult {
                    spec,
                    state: IndexBuildState::Pending,
                })
                .collect(),
        };
        let uri = report.path.to_str().expect("validated UTF-8 destination");
        let mut dataset = match Dataset::open(uri).await {
            Ok(dataset) => dataset,
            Err(error) => {
                return Err(backend(IndexCommitError {
                    report,
                    message: error.to_string(),
                }));
            }
        };
        report.data_version = Some(dataset.version_id());
        report.latest_version = report.data_version;
        build(&mut dataset, report).await.map_err(backend)?;
        Ok(summary)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IndexBuildState {
    Pending,
    Complete { uuid: String },
    Failed { error: String },
}

#[derive(Debug, Clone)]
pub struct IndexBuildResult {
    pub spec: LanceIndexSpec,
    pub state: IndexBuildState,
}

/// Failure details for index construction after data has already committed.
#[derive(Debug, Clone)]
pub struct IndexBuildReport {
    pub path: PathBuf,
    pub rows_written: u64,
    pub data_version: Option<u64>,
    pub latest_version: Option<u64>,
    pub indexes: Vec<IndexBuildResult>,
}

/// Returned inside `open_arrow_sink_core::Error::Backend`; downcast to inspect.
#[derive(Debug)]
pub struct IndexCommitError {
    pub report: IndexBuildReport,
    pub message: String,
}

impl fmt::Display for IndexCommitError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "data committed at {:?}; indexing {} failed: {}",
            self.report.data_version,
            self.report.path.display(),
            self.message
        )
    }
}
impl std::error::Error for IndexCommitError {}

async fn build(
    dataset: &mut Dataset,
    mut report: IndexBuildReport,
) -> std::result::Result<IndexBuildReport, IndexCommitError> {
    for index in &mut report.indexes {
        let result = create_index(dataset, &index.spec).await;
        match result {
            Ok(uuid) => {
                index.state = IndexBuildState::Complete { uuid };
                report.latest_version = Some(dataset.version_id());
            }
            Err(error) => {
                let message = format!("{}: {error}", index.spec.name());
                index.state = IndexBuildState::Failed {
                    error: error.to_string(),
                };
                // Retain the last confirmed version if publication is uncertain.
                return Err(IndexCommitError { report, message });
            }
        }
    }
    Ok(report)
}

async fn create_index(dataset: &mut Dataset, spec: &LanceIndexSpec) -> lance::Result<String> {
    let (kind, params): (_, Box<dyn IndexParams>) = match spec.kind {
        LanceIndexKind::Scalar => (
            IndexType::BTree,
            Box::new(ScalarIndexParams::for_builtin(BuiltinIndexType::BTree)),
        ),
        LanceIndexKind::Vector => {
            let rows = dataset.count_rows(None).await?;
            (
                IndexType::IvfFlat,
                Box::new(VectorIndexParams::ivf_flat(
                    rows.div_ceil(8192).clamp(1, 4096),
                    MetricType::L2,
                )),
            )
        }
        LanceIndexKind::Text => (
            IndexType::Inverted,
            Box::new(InvertedIndexParams::default()),
        ),
        LanceIndexKind::BloomFilter => (
            IndexType::BloomFilter,
            Box::new(ScalarIndexParams::for_builtin(
                BuiltinIndexType::BloomFilter,
            )),
        ),
    };
    // Never replace an existing index.
    let index = dataset
        .create_index(
            &[&spec.column],
            kind,
            Some(spec.name()),
            params.as_ref(),
            false,
        )
        .await?;
    Ok(index.uuid.to_string())
}

fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write;
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        write!(&mut encoded, "{byte:02x}").unwrap();
    }
    encoded
}
