//! Build index files without publishing a dataset version.
use lance::{
    Dataset,
    index::{CreateIndexBuilder, DatasetIndexExt, vector::VectorIndexParams},
};
use lance_index::{
    IndexParams, IndexType as LanceIndexType,
    scalar::{BuiltinIndexType, InvertedIndexParams, ScalarIndexParams},
};
use lance_linalg::distance::MetricType;
use lance_table::format::IndexMetadata;
use open_arrow_sink_core::{ExistingIndexPolicy, IndexSpec, IndexType};
use tokio::sync::OnceCell;

use crate::error::index_creation;

const TARGET_VECTOR_PARTITION_ROWS: usize = 8192;
const MAX_VECTOR_PARTITIONS: usize = 4096;

pub(crate) async fn build_indexes(
    dataset: &mut Dataset,
    indexes: &[IndexSpec],
) -> lance::Result<Vec<IndexMetadata>> {
    let vector_rows = OnceCell::new();
    let mut built = Vec::new();
    // Bound memory to one native build and stop before publication on failure.
    for spec in indexes {
        if let Some(index) = create_index(dataset, spec, &vector_rows).await? {
            built.push(index);
        }
    }
    Ok(built)
}

async fn create_index(
    dataset: &mut Dataset,
    spec: &IndexSpec,
    vector_rows: &OnceCell<lance::Result<usize>>,
) -> lance::Result<Option<IndexMetadata>> {
    if spec.existing == ExistingIndexPolicy::Keep
        && dataset
            .load_indices()
            .await
            .map_err(|error| index_creation(spec, error))?
            .iter()
            .any(|i| i.name == index_name(spec))
    {
        return Ok(None);
    }
    let (lance_type, params): (_, Box<dyn IndexParams>) = match spec.index_type {
        IndexType::Scalar => (
            LanceIndexType::BTree,
            Box::new(ScalarIndexParams::for_builtin(BuiltinIndexType::BTree)),
        ),
        IndexType::Vector => {
            // Every build uses the same unpublished candidate snapshot. Cache
            // the complete result so multiple vector requests also share failures.
            // Keep requests that find an existing index never initialize this cell.
            let rows = vector_rows
                .get_or_init(|| dataset.count_rows(None))
                .await
                .as_ref()
                .map_err(|error| index_creation(spec, error))?;
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
    // Write files only; the commit handler publishes index metadata and data
    // fragments together in the final manifest.
    CreateIndexBuilder::new(dataset, &[&spec.column], lance_type, params.as_ref())
        .name(index_name(spec))
        .replace(spec.existing == ExistingIndexPolicy::Rebuild)
        .execute_uncommitted()
        .await
        .map(Some)
        .map_err(|error| index_creation(spec, error))
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
