# Lance dataset sink

`LanceDatasetSink` creates a new local Lance dataset from Arrow batches. It requires
a Tokio runtime and a new UTF-8 directory path whose parent already exists.
Existing paths are rejected. Keep exclusive ownership of the destination until the
session ends, including index construction and any retries.

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
reserved directory. Dropping closes the stream with an error and never commits,
but can leave orphan files for manual cleanup. Commit failures/cancellation can
have an uncertain publication outcome; do not automatically retry.

This first version supports create-only local datasets and unordered input.
No append, overwrite, schema evolution, cloud options or ordering guarantee.
The destination may contain uncommitted files before commit, but becomes readable
as a Lance dataset only when the native manifest is published.

A manual write/read example (destination must not already exist):

```sh
cargo run -p open-arrow-sink-lance --example write_lance -- /tmp/new-example.lance
```

The native Lance dependency needs `protoc` available when building from source.

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

A runnable index construction example (destination must not exist):

```sh
cargo run -p open-arrow-sink-lance --example write_indexed -- /tmp/indexed-lance
```
