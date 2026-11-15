# Storage operator setup benchmark

Baseline: `18c8655`. Measured on macOS 26.5.2, arm64, Rust 1.97.1,
Cargo bench profile, locked dependencies, default features.

```sh
cargo bench -p open-arrow-sink-storage --bench operator
```

For the baseline, run the same harness and manifest bench entry on `18c8655`
without the storage changes. There is one warmup followed by five samples.
`operator` obtains and drops the returned operator 1,000,000 times per sample,
using black-box inputs/outputs. `stat_optional` performs 10,000 filesystem metadata
requests per sample against a four-byte file. Setup and cleanup are excluded.

| Operation | Baseline median ns/op | Reused median ns/op | Ratio |
| --- | --- | --- | --- |
| `operator` | 227.970 | 10.690 | 21.33x |
| `stat_optional` | 10088.521 | 9712.804 | 1.04x |

Raw samples: [baseline](results/baseline.csv), [reused](results/reused.csv).

`Storage::operator()` previously cloned and layered the OpenDAL operator on every
call. Adding a layer replays the composed layer stack and allocates a transport
wrapper even for filesystem operations. The change builds that wrapper once per
storage configuration and returns a clone. The original operator is retained so
replacing the HTTP budget rebuilds from the caller's layer stack without nesting
old budgets. Clones and returned operators continue sharing their configured budget.

The roughly 21x improvement applies only to the CPU microbenchmark. The filesystem
metadata control changes by about 4%, which is small relative to run-to-run noise;
no significant filesystem throughput improvement is claimed. Network latency will
usually dwarf this setup cost. Retaining the base and configured operators adds
shared context handles per `Storage`; this does not copy backend contents.

Regression tests exercise shared budgets across returned operators and Storage
clones, repeated application of the same budget, and replacement while old
operators still hold permits. The storage, layer and Parquet suites are also run
with all features and localhost networking for their HTTP fixtures.
