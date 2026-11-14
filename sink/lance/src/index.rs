//! Lance implementation of shared index requests, run after the data commit.
use lance::{
    Dataset,
    index::{DatasetIndexExt, vector::VectorIndexParams},
};
use lance_index::{
    IndexParams, IndexType as LanceIndexType,
    scalar::{BuiltinIndexType, InvertedIndexParams, ScalarIndexParams},
};
use lance_linalg::distance::MetricType;
use open_arrow_sink_core::{
    IndexBuildReport, IndexBuildResult, IndexBuildState, IndexCommitError, IndexSpec, IndexType,
};

pub(crate) async fn build_indexes(
    dataset: &mut Dataset,
    destination: String,
    rows_written: u64,
    indexes: Vec<IndexSpec>,
) -> std::result::Result<(), IndexCommitError> {
    if indexes.is_empty() {
        return Ok(());
    }
    let version = dataset.version_id();
    let report = IndexBuildReport {
        destination,
        rows_written,
        data_version: Some(version),
        latest_version: Some(version),
        indexes: indexes
            .into_iter()
            .map(|spec| IndexBuildResult {
                spec,
                state: IndexBuildState::Pending,
            })
            .collect(),
    };
    build(dataset, report).await.map(|_| ())
}

async fn build(
    dataset: &mut Dataset,
    mut report: IndexBuildReport,
) -> std::result::Result<IndexBuildReport, IndexCommitError> {
    for index in &mut report.indexes {
        let result = create_index(dataset, &index.spec).await;
        match result {
            Ok(uuid) => {
                index.state = IndexBuildState::Complete { index_id: uuid };
                report.latest_version = Some(dataset.version_id());
            }
            Err(error) => {
                let message = format!("{}: {error}", index_name(&index.spec));
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

async fn create_index(dataset: &mut Dataset, spec: &IndexSpec) -> lance::Result<String> {
    let (lance_type, params): (_, Box<dyn IndexParams>) = match spec.index_type {
        IndexType::Scalar => (
            LanceIndexType::BTree,
            Box::new(ScalarIndexParams::for_builtin(BuiltinIndexType::BTree)),
        ),
        IndexType::Vector => {
            let rows = dataset.count_rows(None).await?;
            (
                LanceIndexType::IvfFlat,
                Box::new(VectorIndexParams::ivf_flat(
                    rows.div_ceil(8192).clamp(1, 4096),
                    MetricType::L2,
                )),
            )
        }
        IndexType::Text => (
            LanceIndexType::Inverted,
            Box::new(InvertedIndexParams::default()),
        ),
        IndexType::BloomFilter => (
            LanceIndexType::BloomFilter,
            Box::new(ScalarIndexParams::for_builtin(
                BuiltinIndexType::BloomFilter,
            )),
        ),
    };
    // Never replace an existing index.
    let index = dataset
        .create_index(
            &[&spec.column],
            lance_type,
            Some(index_name(spec)),
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

/// Native names reflect Lance's implementation of each logical index category.
fn index_name(spec: &IndexSpec) -> String {
    let kind = match spec.index_type {
        IndexType::Scalar => "btree",
        IndexType::Vector => "ivf_flat",
        IndexType::Text => "text",
        IndexType::BloomFilter => "bloom",
    };
    format!("oas_{kind}_{}", hex(spec.column.as_bytes()))
}
