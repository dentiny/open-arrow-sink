//! Lance implementation of shared index requests, run after the data commit.
use arrow_schema::DataType;
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
const MAX_PRODUCT_SUBVECTORS: usize = 16;

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
        IndexType::Vector => {
            let rows = dataset.count_rows(None).await?;
            let DataType::FixedSizeList(_, dimension) = dataset
                .schema()
                .field(&spec.column)
                .expect("index column validated at open")
                .data_type()
            else {
                unreachable!("vector column type validated at open");
            };
            (
                LanceIndexType::IvfPq,
                Box::new(VectorIndexParams::with_ivf_pq_params(
                    MetricType::L2,
                    IvfBuildParams::new(
                        rows.div_ceil(TARGET_VECTOR_PARTITION_ROWS)
                            .clamp(1, MAX_VECTOR_PARTITIONS),
                    ),
                    PQBuildParams {
                        num_sub_vectors: product_subvectors(dimension as usize),
                        ..Default::default()
                    },
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
        IndexType::Vector => "ivf_pq",
        IndexType::Text => "text",
        IndexType::BloomFilter => "bloom",
    };
    format!("oas_{kind}_{}", hex(spec.column.as_bytes()))
}

// PQ splits each vector into equal-width sub-vectors. Bound the code size
// without requiring dimensions to be multiples of 16.
fn product_subvectors(dimension: usize) -> usize {
    (1..=MAX_PRODUCT_SUBVECTORS.min(dimension))
        .rev()
        .find(|candidate| dimension.is_multiple_of(*candidate))
        .expect("positive vector dimension validated at open")
}

#[cfg(test)]
mod tests {
    use super::product_subvectors;

    #[rstest::rstest]
    #[case::standard_dimension(128, 16)]
    #[case::composite_dimension(12, 12)]
    #[case::prime_dimension(17, 1)]
    #[test]
    fn chooses_largest_bounded_divisor(#[case] dimension: usize, #[case] expected: usize) {
        assert_eq!(product_subvectors(dimension), expected);
    }
}
