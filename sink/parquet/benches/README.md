# Parquet small-batch benchmark

Baseline: `18c8655`; optimized: coalesce input batches rather than flushing a row
group after every write. Measured on macOS 26.5.2, arm64, Rust 1.97.1,
Cargo bench profile, locked dependencies, default features and uncompressed data.

Run:

```sh
cargo bench -p open-arrow-sink-parquet --bench write_batches
```

For the baseline, use the same harness and manifest bench entry on `18c8655`
without the writer changes. Each case writes 262,144 rows (sequential Int64 plus
18-byte strings with 4,096 distinct values), with one warmup and five samples.
Timing includes filesystem destination open, writing and commit. Data generation,
metadata inspection and temporary-directory cleanup are excluded. Data fits in the
OS cache; these are local write/encoding results, not durable disk or cloud throughput.

| Batch rows | File target | Baseline median ms | Optimized median ms | Speedup | File bytes | Row groups |
| --- | --- | --- | --- | --- | --- | --- |
| 64 | None | 52.795 | 19.497 | 2.71x | 10068819 → 2852411 | 4096 → 1 |
| 1024 | None | 32.124 | 17.141 | 1.87x | 8638277 → 2852112 | 256 → 1 |
| 8192 | None | 22.579 | 16.623 | 1.36x | 5816314 → 2852112 | 32 → 1 |
| 64 | 256 MiB | 51.208 | 18.668 | 2.74x | 10068819 → 2852411 | 4096 → 1 |
| 1024 | 256 MiB | 27.127 | 16.624 | 1.63x | 8638277 → 2852112 | 256 → 1 |
| 8192 | 256 MiB | 23.478 | 17.567 | 1.34x | 5816314 → 2852112 | 32 → 1 |

Raw samples: [baseline](results/baseline.csv), [optimized](results/coalesced.csv).
The string dictionary spans batches after coalescing, reducing repeated dictionary
and row-group metadata overhead. Results depend on data cardinality, row width,
compression and hardware; no Lance, Iceberg or network throughput claim is made.

## Buffering tradeoff

The old implementation flushes every batch. The optimized writer retains an
in-progress Parquet row group up to the backend default of 1,048,576 rows, or
flushes early when estimated encoded bytes plus flushed bytes reach an explicit
file target. Rolling still tests actual flushed bytes before a nonempty batch.
Memory can increase for small input batches; the row limit is not a byte limit.
Wide or high-cardinality schemas need separate memory sizing. Commit flushes the
last group; write acceptance does not imply the batch has reached storage.

Validation covers nullable/string round trips with default and large targets,
automatic row-group boundaries across 1,056,768 rows, tiny-target rolling, parallel
sessions, compression, append/overwrite and abort. All 14 Parquet integration tests
pass with all features enabled.
