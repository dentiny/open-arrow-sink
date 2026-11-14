# Lance dataset sink

`LanceDatasetSink` creates a new local Lance dataset from Arrow batches. It requires
a Tokio runtime and a new UTF-8 directory path whose parent already exists.
Existing paths are rejected. Keep exclusive ownership of the destination until the
session ends, including index building and any retries.

```rust,ignore
use open_arrow_sink::{Ordering, Sink, WriteOptions, lance::LanceDatasetSink};

let mut writer = LanceDatasetSink.open(
    "./new.lance".into(),
    schema,
    WriteOptions { ordering: Ordering::Unordered, ..WriteOptions::default() },
).await?;
writer.write(batch).await?;
let summary = writer.commit().await?;
```

Enable the facade's `sink-lance` feature, or depend on `open-arrow-sink-lance`
directly. Cloud backends in Lance are disabled.

`WriteOptions.target_file_size` sets Lance's native `max_bytes_per_file` parameter.
`None` keeps the SDK default. It is a soft data-file target: buffered pages, groups,
and file footers can make files larger; Lance's row-count limits also still apply.

Writes feed a capacity-one queue into Lance's native streaming writer. Native
buffering is bounded by SDK defaults; this adapter never collects all input
batches. `write` means accepted by the queue; encoding errors may surface on a
later write or commit. Only `commit` sends a successful end-of-stream and allows
Lance to publish its first manifest. `abort` stops the worker and removes the
reserved directory when `cleanup_on_error` is enabled. Dropping closes the stream
with an error and never commits, but can leave orphan files for manual cleanup.
Commit failures/cancellation can have an uncertain publication outcome; do not
automatically retry data writes.

The sink supports create-only local datasets and unordered input. Append,
overwrite, schema evolution, cloud options, and input-order guarantees are outside
this implementation. The destination may contain uncommitted files before commit,
but becomes readable as a Lance dataset only when the native manifest is published.

## Index layer

`LanceIndexLayer` wraps `LanceDatasetSink` and creates indexes after the data commit.
It supports Scalar (BTree), Vector (IVF_FLAT with L2), Text (Inverted), and BloomFilter.
The index list is validated against the fixed session schema before a destination
is created. Missing columns, duplicate column definitions, and unsupported types
are rejected. Vector columns must be fixed-size lists of Float16, Float32, or
Float64 with a positive dimension. Types are checked against Lance 14.0.1 rather
than blindly inheriting the older reference's vector type matrix.

```rust,ignore
use open_arrow_sink::{SinkExt, lance::{LanceDatasetSink, LanceIndexKind, LanceIndexLayer, LanceIndexSpec}};

let sink = LanceDatasetSink.layer(LanceIndexLayer::new(vec![
    LanceIndexSpec::new("id", LanceIndexKind::Scalar),
    LanceIndexSpec::new("embedding", LanceIndexKind::Vector),
]));
```

Each column can have one requested index. Names are deterministic and existing
indexes are never replaced. Index builds run sequentially on the committed dataset.
The vector IVF partition count is `clamp(ceil(dataset_rows / 8192), 1, 4096)`.
Training failures, including insufficient usable vectors, are returned explicitly.
An empty index list preserves the underlying sink behavior.

Index creation can publish additional dataset versions. If it fails, the data and
previously created indexes remain visible, regardless of `cleanup_on_error`.
`Error::Backend` contains an `IndexCommitError` with an `IndexBuildReport`: destination,
row count, original data version, latest confirmed version, and each index's state.
A failure to reopen the committed dataset leaves the version fields unknown.

```rust,ignore
use open_arrow_sink::{Error, lance::{IndexCommitError, retry_indexes}};

if let Err(Error::Backend(error)) = writer.commit().await {
    if let Some(error) = error.downcast_ref::<IndexCommitError>() {
        // Persist the report if recovery must survive a process restart.
        // Reconcile the cause first; a permanent training error will fail again.
        let report = retry_indexes(error.report.clone()).await?;
    }
}
```

`retry_indexes` never writes rows and skips successful indexes after checking their
UUIDs and names. It requires the current dataset version to match the report's
latest confirmed version. A changed version or uncertain index publication requires
caller reconciliation; retry does not silently replace an index or accept stale
state. Exclusive destination ownership is required, including during retry. These
checks are not a distributed lock against external writers.

## Partition layer

Install `LancePartitionLayer` outside the index layer so that each partition gets
its own dataset and indexes. Both layers are provided by the Lance crate. The
partition layer accepts only the sealed `LanceSink` implementations: the base
Lance sink and its indexed variant. This prevents accidental use with other
backends, nested partition layers, or an index applied to the partition root.
General ordering and rebatch layers can wrap the resulting sink on the outside.

```rust,ignore
use std::num::NonZeroUsize;
use open_arrow_sink::{SinkExt, lance::LancePartitionLayer};

let sink = sink.layer(LancePartitionLayer::new(
    vec!["region".into()],
    NonZeroUsize::new(32).unwrap(),
));
```

The request is a fresh root directory; its parent must exist. Rows are routed by
one or more string (Utf8/LargeUtf8), integer, or boolean columns. Partition columns
remain in the stored schema. Nulls and empty strings have distinct keys. Each
partition is a child directory whose name encodes key values as hex with explicit
null markers. Use `PartitionKey::directory_name()` to resolve a key within a fixed
partition schema and column order. Encoded keys over 200 bytes are rejected rather
than truncated. Raw key values never become filesystem paths.

The root is reserved at open, but child datasets are created lazily. Empty input
creates no child datasets. Every batch is schema-validated, including empty batches.
All index and partition configuration is validated before reserving the root.

`max_partitions` bounds active dataset writers and retained partition metadata;
there is no early commit or eviction. Exceeding the limit makes the session terminal.
The default input batch budget is 64 MiB of retained Arrow buffers; override it with
`with_max_batch_bytes(NonZeroUsize)`. Slices can retain larger buffers and are charged
accordingly. Routing indices and selected batches add temporary memory, and each
active native Lance writer has its own SDK buffers. The input budget is not a
process-wide memory limit. Queue sends provide backpressure; writes and partition
commits are driven sequentially without additional background coordination tasks.

On commit, each partition's native writer publishes its data and then its indexes.
All partitions are attempted, even if another partition fails. The root is not a
single Lance dataset and there is no global index or atomic commit across partitions.
`Error::Backend` contains a `PartitionCommitError` on partial failure, with keys,
paths, and completion or failure details for every partition. An index failure
includes its `IndexBuildReport` for a separate index-only retry. A native data commit
failure can have an unknown publication outcome. Never replay the whole partitioned
append/write operation based solely on this aggregate error.

Write errors or cancellation make the partition session terminal. Calling commit
after a write error aborts instead of publishing a subset. Explicit abort stops
all workers and, when cleanup is enabled, removes only the reserved root. Dropping
never commits and may leave uncommitted files. Cancellation during commit may leave
some partitions visible and requires inspection before recovery.

A runnable partition and index example (destination must not exist):

```sh
cargo run -p open-arrow-sink-lance --example write_partitioned -- /tmp/partitioned-lance
```

A basic write/read example is also available as `--example write_lance`.
The native Lance dependency needs `protoc` available when building from source.
