# Lance dataset sink

`LanceDatasetSink` writes local Lance datasets from Arrow batches. It requires
a Tokio runtime and a UTF-8 directory path whose parent already exists.
Create mode (the default) rejects existing paths. Keep exclusive ownership of the destination until the
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
Lance to publish a manifest. `abort` stops the worker and removes only a newly
reserved directory when cleanup is enabled. Dropping closes the stream with an error and never commits,
but can leave orphan files for manual cleanup. Commit failures/cancellation can
have an uncertain publication outcome; do not automatically retry.

This sink supports create, append, and full overwrite of local datasets with
unordered input. It does not provide append-time schema evolution, cloud options,
or an ordering guarantee. New fragments may exist before commit; readers see
the new dataset version only when the native manifest is published.

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

## Compression

Supports global and per-column `Compression::Zstd { level }` and
`Compression::Uncompressed`, directly in `WriteOptions.compression` or through
`CompressionLayer`. Zstandard levels 0 through 22 are supported; `None` uses
Lance's codec default. Unsupported codecs and levels fail before destination
creation. Empty compression options preserve existing field metadata and native
defaults.

Column keys are exact top-level field names accepted by Lance. A column override
replaces the entire global setting, including its level, and applies recursively
to nested descendants. Explicit settings replace existing compression metadata
and clear old levels when no level is requested. Unrelated schema and field
metadata are retained.

The adapter sets `lance-encoding:compression` and
`lance-encoding:compression-level` in an internal Arrow schema, including the
separate dictionary-value compression settings. `Uncompressed` disables native
general-purpose compression; Lance still chooses its structural encodings.
`writer.schema()` and input batch validation use the caller's original schema.
Batches reuse their existing value buffers with the internal schema, and readback
includes the persisted encoding metadata. These settings survive file rolling.

Snappy and Gzip are unsupported by Lance. `Lz4Raw` is not advertised: Lance's
stable format can substitute Zstd for LZ4 on per-value paths, so it cannot honor
that shared codec request consistently.

```rust
use open_arrow_sink_core::{Compression, CompressionOptions};

let compression = CompressionOptions {
    global: Some(Compression::Zstd { level: Some(9) }),
    columns: [("raw_payload".into(), Compression::Uncompressed)].into(),
};
```

### Write modes

`WriteOptions.mode` accepts `Some(WriteMode::Create)` (default when omitted),
`Some(WriteMode::Append)`, and `Some(WriteMode::Overwrite)`. Append requires an
existing dataset with the exact effective schema, including requested compression
metadata. Reuse the original compression options when appending. Overwrite publishes a new native
Lance version replacing all rows and permits a new schema; it can also create a
missing dataset. An existing path that is not a Lance dataset is rejected.

Append and overwrite use Lance's native versioned commit, so old rows remain
readable until commit and an empty overwrite clears the dataset. Abort never
recursively removes an existing dataset. Unpublished fragments from aborted
writes to existing datasets may remain for native Lance maintenance to reclaim.
For a newly reserved directory, abort removes it when `cleanup_on_error` is true.
Index construction still follows data publication; an index failure does not roll
back the committed data version. Callers must exclude external replacement of
the dataset directory during a session.
