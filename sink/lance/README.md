# Lance dataset sink

`LanceDatasetSink` creates a new local Lance dataset from Arrow batches. It requires
a Tokio runtime and a new UTF-8 directory path whose parent already exists.
Existing paths are rejected. Keep exclusive ownership of the destination until the
session ends, including index construction.

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

## Index construction

Set the shared `WriteOptions.indexes` list when opening `LanceDatasetSink`:

```rust,ignore
use open_arrow_sink::{IndexKind, IndexSpec, Ordering, Sink, WriteOptions, lance::LanceDatasetSink};

let mut writer = LanceDatasetSink.open(
    "./indexed.lance".into(),
    schema,
    WriteOptions {
        ordering: Ordering::Unordered,
        indexes: vec![
            IndexSpec::new("id", IndexKind::Scalar),
            IndexSpec::new("embedding", IndexKind::Vector),
        ],
        ..WriteOptions::default()
    },
).await?;
writer.write(batch).await?;
let summary = writer.commit().await?;
```

`IndexKind`, `IndexSpec`, construction results, and `IndexCommitError` are defined
in `open-arrow-sink-core` and re-exported by the facade. They have no Lance SDK
dependency. Lance advertises `SinkCapability.indexes = true`; Parquet and Iceberg
currently advertise `false` and reject index requests before destination I/O.

The shared `validation.rs` checks index column existence and uniqueness. Lance's
`validation.rs` checks each requested kind against its supported Arrow types before
creating a directory or writer. Each column can have one requested index. Lance's
concrete mappings are:

| Shared kind | Lance implementation |
| --- | --- |
| Scalar | BTree |
| Vector | IVF_FLAT with L2 |
| Text | Inverted with default parameters |
| BloomFilter | Bloom filter |

Vector columns must be fixed-size lists of Float16, Float32, or Float64 with a
positive dimension. The default IVF partition count is
`clamp(ceil(dataset_rows / 8192), 1, 4096)`. Unsupported types fail before writing;
training failures, including insufficient usable vectors, are returned explicitly.
An empty index list preserves the underlying sink behavior.

Index creation uses the dataset handle returned by the native data commit. It runs
sequentially and may publish additional versions. Names are deterministic and existing
indexes are never replaced. If construction fails, the data and previously created
indexes remain visible regardless of `cleanup_on_error`. `Error::Backend` contains
an `IndexCommitError` with a shared `IndexBuildReport`: destination, row count,
original data version, latest confirmed version, and each requested index's state.
The complete state carries an opaque backend index ID; for Lance this is its UUID.
The failed index and any remaining pending indexes are reported without retrying.

A runnable index construction example (destination must not exist):

```sh
cargo run -p open-arrow-sink-lance --example write_indexed -- /tmp/indexed-lance
```
