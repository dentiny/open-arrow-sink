//! Lance implementation of shared index requests, run after the data commit.
use lance::{
    Dataset,
    index::{DatasetIndexExt, vector::VectorIndexParams},
};
use lance_index::{
    IndexParams, IndexType as LanceIndexType,
    scalar::{BuiltinIndexType, InvertedIndexParams, ScalarIndexParams},
    vector::{ivf::IvfBuildParams, pq::PQBuildParams},
};
use lance_linalg::distance::MetricType;
use open_arrow_sink_core::{IndexCommitError, IndexSpec, IndexType};
use tokio::task::JoinSet;

use crate::error::{index_creation, index_refresh, index_task};

const TARGET_VECTOR_PARTITION_ROWS: usize = 8192;
const MAX_VECTOR_PARTITIONS: usize = 4096;

pub(crate) async fn build_indexes(
    dataset: &mut Dataset,
    indexes: &[IndexSpec],
) -> std::result::Result<(), IndexCommitError> {
    if indexes.is_empty() {
        return Ok(());
    }
    let mut tasks = JoinSet::new();
    for spec in indexes {
        let mut dataset = dataset.clone();
        let spec = spec.clone();
        tasks.spawn(async move {
            create_index(&mut dataset, &spec)
                .await
                .map_err(|error| index_creation(&spec, error))
        });
    }

    // Drain every started task even after an error, so returning never detaches
    // index commits that could keep modifying the dataset in the background.
    let mut first_error = None;
    while let Some(result) = tasks.join_next().await {
        let result = result.unwrap_or_else(|error| Err(index_task(error)));
        if let Err(error) = result {
            first_error.get_or_insert(error);
        }
    }
    // Each task commits through its own handle. Refresh the caller's snapshot
    // after all tasks finish, including when only some indexes succeeded.
    if let Err(error) = dataset.checkout_latest().await {
        first_error.get_or_insert_with(|| index_refresh(error));
    }
    match first_error {
        Some(error) => Err(error),
        None => Ok(()),
    }
}

async fn create_index(dataset: &mut Dataset, spec: &IndexSpec) -> lance::Result<()> {
    let (lance_type, params): (_, Box<dyn IndexParams>) = match spec.index_type {
        IndexType::Scalar => (
            LanceIndexType::BTree,
            Box::new(ScalarIndexParams::for_builtin(BuiltinIndexType::BTree)),
        ),
        IndexType::Vector | IndexType::VectorPq => {
            let rows = dataset.count_rows(None).await?;
            let partitions = rows
                .div_ceil(TARGET_VECTOR_PARTITION_ROWS)
                .clamp(1, MAX_VECTOR_PARTITIONS);
            if spec.index_type == IndexType::Vector {
                (
                    LanceIndexType::IvfFlat,
                    Box::new(VectorIndexParams::ivf_flat(partitions, MetricType::L2)),
                )
            } else {
                let field = dataset
                    .schema()
                    .field(&spec.column)
                    .ok_or_else(|| lance::Error::invalid_input("vector column not found"))?;
                let arrow_schema::DataType::FixedSizeList(_, dimension) = field.data_type() else {
                    return Err(lance::Error::invalid_input(
                        "IVF_PQ requires a fixed-size vector",
                    ));
                };
                let dimension = usize::try_from(dimension)
                    .map_err(|_| lance::Error::invalid_input("invalid vector dimension"))?;
                let num_sub_vectors = (1..=16.min(dimension))
                    .rev()
                    .find(|n| dimension.is_multiple_of(*n))
                    .unwrap_or(1);
                (
                    LanceIndexType::IvfPq,
                    Box::new(VectorIndexParams::with_ivf_pq_params(
                        MetricType::L2,
                        IvfBuildParams {
                            num_partitions: Some(partitions),
                            ..Default::default()
                        },
                        PQBuildParams {
                            num_sub_vectors,
                            ..Default::default()
                        },
                    )),
                )
            }
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
    dataset
        .create_index(
            &[&spec.column],
            lance_type,
            Some(index_name(spec)),
            params.as_ref(),
            /*replace=*/ false,
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
        IndexType::VectorPq => "ivf_pq",
        IndexType::Text => "text",
        IndexType::BloomFilter => "bloom",
    };
    format!("oas_{kind}_{}", hex(spec.column.as_bytes()))
}
