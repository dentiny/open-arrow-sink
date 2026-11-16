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
use tokio::task::JoinSet;

use crate::error::{index_creation, index_refresh, index_task};

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
    let mut tasks = JoinSet::new();
    for spec in indexes {
        let mut dataset = dataset.clone();
        let spec = spec.clone();
        let summary = summary.clone();
        tasks.spawn(async move {
            create_index(&mut dataset, &spec)
                .await
                .map_err(|error| index_creation(&spec, error, summary))
        });
    }

    // Drain every started task even after an error, so returning never detaches
    // index commits that could keep modifying the dataset in the background.
    let mut first_error = None;
    while let Some(result) = tasks.join_next().await {
        let result = result.unwrap_or_else(|error| Err(index_task(error, summary.clone())));
        if let Err(error) = result {
            first_error.get_or_insert(error);
        }
    }
    // Each task commits through its own handle. Refresh the caller's snapshot
    // after all tasks finish, including when only some indexes succeeded.
    if let Err(error) = dataset.checkout_latest().await {
        first_error.get_or_insert_with(|| index_refresh(error, summary.clone()));
    }
    match first_error {
        Some(error) => Err(error),
        None => Ok(()),
    }
}

pub(crate) async fn validate_existing_indexes(
    dataset: &Dataset,
    indexes: &[IndexSpec],
) -> open_arrow_sink_core::Result<()> {
    let existing = dataset
        .load_indices()
        .await
        .map_err(crate::error::backend)?;
    for spec in indexes {
        if spec.existing == ExistingIndexPolicy::Error
            && existing.iter().any(|i| i.name == index_name(spec))
        {
            return Err(open_arrow_sink_core::Error::Unsupported(format!(
                "index {} already exists; select Keep or Rebuild",
                index_name(spec)
            )));
        }
    }
    Ok(())
}

async fn create_index(dataset: &mut Dataset, spec: &IndexSpec) -> lance::Result<()> {
    if spec.existing == ExistingIndexPolicy::Keep
        && dataset
            .load_indices()
            .await?
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
            let rows = dataset.count_rows(None).await?;
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
        .await?;
    Ok(())
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
