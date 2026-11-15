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
use open_arrow_sink::{IndexType, IndexSpec, Ordering, Sink, WriteOptions, lance::LanceDatasetSink};

let mut writer = LanceDatasetSink.open(
    "./indexed.lance".into(),
    schema,
    WriteOptions {
        ordering: Ordering::Unordered,
        indexes: vec![
            IndexSpec::new("id", IndexType::Scalar),
            IndexSpec::new("embedding", IndexType::Vector),
        ],
        ..WriteOptions::default()
    },
).await?;
writer.write(batch).await?;
let summary = writer.commit().await?;
```

`IndexType`, `IndexSpec`, and `IndexCommitError` are defined
in `open-arrow-sink-core` and re-exported by the facade. They have no Lance SDK
dependency. Lance lists Scalar, Vector, Text, and BloomFilter in
`SinkCapability.supported_indexes`. Parquet and Iceberg expose empty lists and
reject index requests before destination I/O. Shared validation checks every
requested type against the supported list.

The shared `validation.rs` checks index column existence and uniqueness. Lance's
`validation.rs` checks each requested kind against its supported Arrow types before
creating a directory or writer. Each column can have one requested index. Lance's
concrete mappings are:

| Shared type | Lance implementation |
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

Index creation clones the dataset handle returned by the native data commit and
spawns one Tokio task per requested index. Each task builds and commits its index
through Lance, potentially publishing another dataset version. Names are deterministic
and existing indexes are never replaced. All started tasks are awaited, including
when one fails, and the original dataset handle is refreshed after they finish.
The first observed error is returned after all tasks complete. Successfully built
indexes and committed data remain visible regardless of `cleanup_on_error`.
`Error::Backend` contains an `IndexCommitError` identifying the failed index type
and column, or a task failure if a worker panics. Dropping the commit future aborts
the task set; native publication may still have an uncertain outcome.

A runnable index construction example (destination must not exist):

```sh
cargo run -p open-arrow-sink-lance --example write_indexed -- /tmp/indexed-lance
```

## Concurrent writing

Wrap `LanceDatasetSink` in `UnorderedWriteLayer::new(concurrency)` and explicitly
allow `Ordering::Unordered`. The sink reserves one new directory and returns
independent fragment writers plus one session committer. Each worker feeds a
capacity-one queue into Lance's `InsertBuilder::execute_uncommitted_stream`;
workers write fragments but never publish a manifest. The file-size target applies
to every worker, and a worker can produce multiple fragments.

After every writer finishes, the committer combines the create transactions'
fragments and publishes one data manifest through `CommitBuilder`. Lance assigns
unique fragment IDs at this point. Empty sessions still create a readable empty
dataset. Requested indexes are built once against the combined dataset afterward;
index construction can publish additional versions, with the failure semantics
above. Before publication, abort first stops every worker, then removes the reserved
directory only when `cleanup_on_error` is enabled. Publication failures retain files
because the dataset may already have been committed. Drop never publishes.
