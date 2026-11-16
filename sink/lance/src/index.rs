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
    ExistingIndexPolicy, IndexCommitError, IndexSpec, IndexType, WriteSummary,
};
use tokio::sync::OnceCell;

use crate::error::index_creation;

const TARGET_VECTOR_PARTITION_ROWS: usize = 8192;
const MAX_VECTOR_PARTITIONS: usize = 4096;

pub(crate) async fn build_indexes(
    dataset: &mut Dataset,
    indexes: &[IndexSpec],
    summary: &WriteSummary,
) -> std::result::Result<(), IndexCommitError> {
    if indexes.is_empty() {
        return Ok(());
    }
    let vector_rows = OnceCell::new();
    let mut first_error = None;
    // Reuse the mutable dataset so each request sees preceding index commits.
    // Attempt every request, retaining the first failure in input order.
    for spec in indexes {
        if let Err(error) = create_index(dataset, spec, &vector_rows, summary).await {
            first_error.get_or_insert(error);
        }
    }
    match first_error {
        Some(error) => Err(error),
        None => Ok(()),
    }
}

async fn create_index(
    dataset: &mut Dataset,
    spec: &IndexSpec,
    vector_rows: &OnceCell<lance::Result<usize>>,
    summary: &WriteSummary,
) -> std::result::Result<(), IndexCommitError> {
    if spec.existing == ExistingIndexPolicy::Keep
        && dataset
            .load_indices()
            .await
            .map_err(|error| index_creation(spec, error, summary.clone()))?
            .iter()
            .any(|i| i.name == index_name(spec))
    {
        return Ok(());
    }
    let (lance_type, params): (_, Box<dyn IndexParams>) = match spec.index_type {
        IndexType::Scalar => (
            LanceIndexType::BTree,
            Box::new(ScalarIndexParams::for_builtin(BuiltinIndexType::BTree)),
        ),
        IndexType::Vector => {
            // Index commits do not change the data row count. Cache
            // the complete result so multiple vector requests also share failures.
            // Keep requests that find an existing index never initialize this cell.
            let rows = vector_rows
                .get_or_init(|| dataset.count_rows(None))
                .await
                .as_ref()
                .map_err(|error| index_creation(spec, error, summary.clone()))?;
            (
                LanceIndexType::IvfFlat,
                Box::new(VectorIndexParams::ivf_flat(
                    rows.div_ceil(TARGET_VECTOR_PARTITION_ROWS)
                        .clamp(1, MAX_VECTOR_PARTITIONS),
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
    // Replacement is explicit; the default continues to reject duplicates.
    dataset
        .create_index(
            &[&spec.column],
            lance_type,
            Some(index_name(spec)),
            params.as_ref(),
            /*replace=*/ spec.existing == ExistingIndexPolicy::Rebuild,
        )
        .await
        .map_err(|error| index_creation(spec, error, summary.clone()))?;
    Ok(())
}

/// Native names reflect Lance's implementation of each logical index category.
pub(crate) fn index_name(spec: &IndexSpec) -> String {
    let kind = match spec.index_type {
        IndexType::Scalar => "btree",
        IndexType::Vector => "ivf_flat",
        IndexType::Text => "text",
        IndexType::BloomFilter => "bloom",
    };
    format!("oas_{kind}_{}", hex::encode(spec.column.as_bytes()))
}
