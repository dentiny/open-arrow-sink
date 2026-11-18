# Proposal: metadata-driven parallel ingestion

Status: proposed. This document specifies a contract and rollout plan; it does not implement a public API.

## Motivation and ownership

Use input metadata and target file size to choose initial write concurrency before ingestion. Distinguish estimated total files, concurrent physical writers, and actual file boundaries. Access reports source statistics; the sink owns target size, encoding, resource limits, and rotation. Applications adapt independently defined types; the libraries retain no compile-time dependency.

Companion: [access statistics](https://github.com/dentiny/open-arrow-access/blob/docs/metadata-driven-ingestion-design/docs/design/ingestion-statistics.md).

## Existing building blocks

`WriteOptions.target_file_size` is a positive soft target. `ParallelSink::open_writers` creates independent `DataWriter` values and one `SessionCommitter`. `UnorderedWriteLayer` dispatches batches to available writers with at most one batch per writer in flight.

That layer overlaps asynchronous I/O without spawning CPU workers. Parquet and Iceberg encoding runs in the caller task; Lance may schedule backend workers. More writers alone do not promise CPU parallelism.

## Proposed API

Illustrative sink-owned types:

```rust,ignore
pub enum Estimate<T> { Unknown, Exact(T), Estimated(T) }

pub struct InputStatistics {
    pub rows: Estimate<u64>,
    pub logical_bytes: Estimate<u64>,
    pub source_bytes: Estimate<u64>,
}

pub struct AutoWriteOptions {
    pub max_writers: NonZeroUsize,
    pub fallback_writers: NonZeroUsize,
    pub memory_budget_bytes: Option<u64>,
}

pub struct WritePlan {
    pub estimated_output_bytes: Estimate<u64>,
    pub estimated_file_count: Estimate<u64>,
    pub writer_count: NonZeroUsize,
    pub target_file_size: u64,
}
```

A planning entry point on a parallel-capable sink accepts schema, input statistics, effective write options, and auto-write options. It returns an inspectable plan without consuming records or opening data writers. Backend validation/metadata inspection may still require I/O.

The executable plan retains or validates schema, encoding, effective target size, and resource policy so they cannot silently change between planning and opening. Resolve options after layer overrides and backend defaults. Do not invent a generic target that differs from actual backend rotation.

Automatic writer selection is opt-in; explicit writer counts remain supported. Unsupported backends reject the policy rather than silently ignore it. Keep the existing publication boundary.

## Estimating output size

Use the first applicable source:

1. An explicit application-provided output-byte estimate.
2. Logical bytes multiplied by a destination-specific encoding estimate.
3. Encoded source bytes multiplied by an explicitly justified conversion estimate.
4. Unknown.

The implementation API must expose the output override and estimator configuration; these are omitted from the compact sketch. Plan diagnostics record the selected basis and conversion factor. Exact input sizes normally yield only Estimated output sizes.

Do not assume a universal ratio of 1. Compression, projection, filtering, generated columns, and blob materialization can change volume. Include row/file overhead when available. Zero logical bytes with nonzero rows do not prove zero output. Transformations after statistics collection must update or invalidate the estimate; a URL string is not downloaded payload size.

Sampling is deferred. If later added, make it explicit and bounded, retaining samples for exactly one replay into ingestion.

## File count and concurrency

For expected nonempty output, E is estimated output bytes and T is the positive effective target:

```text
estimated_file_count = max(1, ceil(E / T))
writer_count = max(1, min(estimated_file_count, max_writers, resource_limit))
```

Use checked arithmetic, including ceiling division. Estimated zero remains conservative unless an exact-empty row observation and compatible pipeline semantics establish emptiness. Even trusted empty metadata cannot replace consuming the actual input when no snapshot is bound.

For Unknown size, use fallback_writers clamped to max_writers and resource limits. Reject impossible budgets or invalid options before opening writers. Unknown never means empty.

Example: 100 GiB logical input at an estimated output/logical ratio of 0.4 gives 40 GiB output. A 512 MiB target suggests about 80 files. If max_writers is 16 and resources permit 8, run 8 writers and rotate their files; do not open 80 files simultaneously.

Source partition count does not cap write concurrency: one sequential reader can feed multiple writers. Configure input read concurrency separately.

### Resource semantics

Account for encoding state, row groups/pages, compression and upload buffers, and queued Arrow batches. A backend estimator may derive resource_limit from a memory budget. Without defensible estimates, this is a planning hint rather than a hard memory guarantee; diagnostics must say so. A queue-length bound alone is not a memory cap.

For partitioned output, a writer may maintain several active files. Supporting backends need an active-partition/open-file limit and eviction or rotation policy. Reject configurations whose requested resource contract cannot be met. Estimate file count per destination partition when its distribution is known; a global quotient may underestimate many small partition files.

## Execution

```text
input metadata + effective write policy
                  |
               WritePlan
                  |
Arrow batches -> bounded dispatch -> W DataWriters
                                      |
                               rotating files
                                      |
                              one SessionCommitter
```

First version uses fixed-count parallel sessions. Dispatch each nonempty batch once to an available writer, bound queued input, and keep per-writer access serial. Backends rotate by actual observable encoded size, permitting page, row-group, and footer overshoot. One writer can produce many files. Avoid empty data files and preserve backend-specific empty-session commits.

Batch granularity can limit utilization: one large batch cannot occupy multiple writers without slicing. Optional slicing must preserve every row and account for retained backing buffers; it is not required for first-version correctness.

Honor source and destination ordering requirements. An unordered path requires explicit permission; automatic planning never weakens an ordering contract or authorizes unordered source emission.

## Completion and failure

After accepted writes complete, finish every writer and invoke the shared committer once only if all finishes succeed. Preserve backend publication guarantees; Parquet directory output is not transactional.

On input/write failure, stop admission and perform best-effort abort under the existing lifecycle; persisted files may remain. Cancellation must not leave detached workers. Commit errors can mean uncertain publication and must not be automatically retried.

Estimates never determine how many rows to accept or when input ends. Underestimation creates more files; overestimation may leave idle writers or smaller tail files. Neither permits truncation, replay, or duplicate publication.

## Observability and validation

Expose estimate basis, estimated bytes/files, chosen writers, effective target, and budget limitations before execution. Report actual files, encoded bytes, and committed rows afterward where supported. Observed I/O bytes differ from successfully committed output totals.

Implementation tests cover known/estimated/unknown sizes, exact empty and zero-column input, overflow, resource clamping and impossible budgets, estimate error, option overrides, transformations and blobs, rotation, ordering rejection, writer failure, cancellation, and one commit across writers.

Benchmark input-limited, encoding-limited, and upload-limited workloads separately; measure throughput, peak memory, file-size distribution, and metadata cost.

## Rollout

1. Add sink-owned metadata and a deterministic, inspectable planner.
2. Wire opt-in auto selection into the fixed-count unordered session path.
3. Audit backend rotation and partition/file budgets against the contract.
4. Add output metrics and examples adapting access statistics.

Dynamic writer resizing, automatic ratio learning, and a task-based CPU executor are follow-ups with separate lifecycle/resource designs.
