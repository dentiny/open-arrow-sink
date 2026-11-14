# Open Arrow Sink Initial Design

The first release supports partitioned writes only through the Lance sink, with separate index configuration and build stages. Each logical partition maps to one Lance dataset. Multiple fragment writers can write to the same partition concurrently, with their transactions merged into a single data commit. This layout is the proposed initial design.

## Responsibilities and Boundaries

| Component | Responsibility |
| --- | --- |
| Partition router | Split Arrow RecordBatches by partition key, preserving complete rows, and route them to the appropriate writer |
| Partition coordinator | Manage dataset writers, backpressure, concurrency limits, and completion status for each partition |
| Lance dataset writer | Collect fragment transactions and commit a data version for one partition |
| Lance index plan | Validate index configuration and invoke Lance index creation after the data commit |

Indexing is a separate architectural layer, initially implemented inside the Lance backend. The partition coordinator schedules indexing, while the index module encapsulates index parameters and Lance API calls. The common Arrow write path does not depend on Lance index types. Other sinks must reject partition or index configuration before writing.

Logical partitions, Lance fragments, and vector index IVF partitions are distinct concepts. Logical partitions define dataset boundaries; fragments store data within a dataset; IVF partitions are a vector search parameter.

## Configuration and Interface Sketch

The following sketch describes the interface structure. The concrete Rust asynchronous interfaces will be determined during implementation.

```rust
struct LanceWriteOptions {
    // Write mode, storage configuration, file size, and other options.
    indexes: Vec<LanceIndexSpec>,
}

struct LanceIndexSpec {
    column: String,
    kind: LanceIndexKind,
}

enum LanceIndexKind {
    Scalar,       // BTree
    Vector,       // IVF_FLAT with L2 by default
    Text,         // Inverted with Lance defaults
    BloomFilter,
}

// prepare(final_storage_schema, specs) -> LanceIndexPlan
// dataset_writer.commit_data() -> CommittedDataset
// index_plan.build(committed_dataset) -> IndexBuildReport
// index_plan.retry(dataset_uri, expected_version, failed_specs)
//     -> IndexBuildReport
```

All newly created partitions inherit the same index definitions. The initial release allows at most one index per column. The configuration can be extended later; per-partition overrides, multicolumn indexes, custom tokenization, and additional vector index algorithms are outside the initial scope.

Index definitions describe the desired state. Build results are recorded separately: configuration alone does not establish that an index exists. Generate index names deterministically from column names and index kinds, and verify that an existing index with the same name matches the definition before replacing it. This avoids overwriting unrelated user-created indexes.

## Write and Index Lifecycle

1. Compute the final storage schema, including column transformations and generated columns, and validate all index definitions. Missing columns, duplicate definitions, and unsupported types must fail before any data files are produced.
2. Split batches by partition key. Key encoding must distinguish nulls, empty strings, and values containing path characters. Do not concatenate raw key values into storage paths.
3. Fragment writers write uncommitted data and hand completed transactions to their dataset writer. All writers for a partition must finish successfully before that partition can commit. If any writer fails, the partition must not be committed as complete.
4. The dataset writer merges compatible transactions, commits through Lance, and records the dataset URI and actual committed version.
5. Create indexes sequentially on the newly committed dataset, using the updated dataset handle after each index creation. Index tasks for different partitions may run concurrently within a configured limit.
6. The coordinator aggregates data and index results for each partition. A partition is fully successful only when all requested indexes have been created successfully.

Each index covers the entire dataset for its partition. Do not create indexes per batch or fragment. The initial release provides neither global indexes across datasets nor atomic commits across partitions. Readers select partitions first and then use the indexes in the corresponding datasets.

## Index Types and Validation

The supported type matrix follows the reference `LanceIndexPlan`, subject to the Lance version pinned by this project.

| Kind | Implementation | Validation before writing |
| --- | --- | --- |
| Scalar | BTree | Check the scalar types allowed by the reference implementation; do not accept every Arrow type |
| Vector | IVF_FLAT with L2 | Require a fixed-size numeric list with a positive dimension and element types supported by the pinned version |
| Text | Inverted | Require a string type |
| BloomFilter | Bloom filter | Maintain its own supported type matrix rather than accepting all Scalar types |

The default vector index heuristic follows the reference implementation: `num_partitions = clamp(ceil(dataset_rows / 8192), 1, 4096)`, using the total row count of the partition's committed dataset. This formula does not guarantee successful training for small inputs. Empty datasets, all-null vectors, and insufficient training samples must produce explicit results. The initial release returns an index build failure with the committed data version rather than silently skipping a requested index.

## Write Modes

Create, append, and overwrite apply to the partitions touched by the current write. Overwrite leaves untouched partitions in place. Replacing an entire table requires an additional table-level protocol.

After append, requested indexes must cover the entire committed dataset. The initial implementation may rebuild them to achieve this. Existing indexes omitted from the request are not guaranteed to cover newly added fragments; results report only the indexes requested by the current write. Explicit incremental maintenance can be added later.

Record the append base version before the first commit, and use Lance's conflict handling for concurrent commits. Do not swallow conflicts and write the data again. If another writer changes the dataset during indexing, return a conflict with recovery information. Full coordination with external writers modifying the same dataset is outside the initial scope.

Empty input creates no logical partitions. Explicit creation of an empty dataset should be defined separately by the nonpartitioned write interface.

## Failure Results and Recovery

Each partition result includes its `partition_key`, dataset URI, row count, `data_version`, latest known version, and the name, status, and error for each requested index. Preserve data commit results and return them with the overall error even when subsequent indexing fails.

| Failure stage | Visible state | Recovery |
| --- | --- | --- |
| Configuration validation | No data written | Correct the configuration and restart |
| Fragment writing | The partition is uncommitted; orphan files may exist | Confirm that no commit occurred before retrying the write; handle orphan files through Lance's cleanup policy |
| Uncertain data commit response | The commit may have succeeded | Resolve the commit outcome first; if it cannot be established, report an unknown state and do not automatically replay append |
| Index building | Data is committed; some indexes may have succeeded | Check the dataset version and actual index state, then retry only failed indexes |

Index results are maintained during the running operation. Recovery after a process restart requires the caller to retain the results or reconcile the state through dataset index metadata. The initial release does not provide persistent job scheduling or exactly-once writes.

Index retries must verify the expected version and existing index definitions. If the dataset has changed, return a version conflict and require the caller to explicitly initiate maintenance against the new version. Continuing an old task must not be reported as success for the original version. Index creation may commit additional versions, so record both the original data version and the version after indexing completes.

## Concurrency and Resource Limits

Configure separate concurrency limits for fragment writing and index building. The coordinator must bound active writers and buffered bytes, applying upstream backpressure when necessary. For high-cardinality partition keys, fragment writers can be rotated while the same dataset writer continues collecting their transactions. Evicting a writer must not cause an early partition commit.

Build indexes after all data for a partition has been committed, avoiding rebuilds on every batch or writer rotation. Partition metadata and uncommitted transactions can still grow with cardinality. Exceeding the resource budget must produce an explicit error; limiting active writers alone does not bound total memory usage.

## Validation Plan

- Exercise multiple partitions and fragment writers, checking row counts, routing, and index coverage in each dataset.
- Ensure missing columns, duplicate definitions, and invalid types fail before any partition is written.
- Inspect Lance metadata for index types and coverage, and run scalar, text, and vector queries to verify results. Counting version files alone is insufficient.
- Inject index failures and verify that committed data and successful indexes are preserved, with no duplicate append during index retries.
- Cover empty input, small vector samples, index rebuilding after append, partition-scoped overwrite, uncertain commit responses, and concurrent version conflicts.
- Verify writer and index task concurrency limits, plus backpressure or resource budget errors for high-cardinality partitions.

## Reference Implementation

The reference is `dentiny/duckdb_lance_conversion` at commit `ba481d653f64875d5825d1b5409ed281206a74d1`.

- [index.rs](https://github.com/dentiny/duckdb_lance_conversion/blob/ba481d653f64875d5825d1b5409ed281206a74d1/rust/src/sink/lance/index.rs): Index kinds, validation before writing, Lance creation parameters, and vector partition count calculation.
- [dataset.rs](https://github.com/dentiny/duckdb_lance_conversion/blob/ba481d653f64875d5825d1b5409ed281206a74d1/rust/src/sink/lance/dataset.rs): Fragment transaction merging and data commits followed by index creation.
- [lance.rs](https://github.com/dentiny/duckdb_lance_conversion/blob/ba481d653f64875d5825d1b5409ed281206a74d1/rust/src/sink/lance.rs): Write configuration and the division between dataset writers and fragment writers.

This design extends the reference with partition scheduling, separate data and index results, and index retry semantics.
