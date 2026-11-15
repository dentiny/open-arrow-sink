# Lance dataset sink

`LanceDatasetSink` writes a Lance dataset from Arrow batches at a local or
remote URL. It requires a Tokio runtime. Missing parents are created.
Create rejects existing destinations; Append requires an existing dataset.
Keep exclusive ownership of the destination until the session ends, including
index construction.

```rust,ignore
use open_arrow_sink::{Ordering, Sink, WriteOptions, lance::LanceDatasetSink};

let mut writer = LanceDatasetSink::default().open(
    "file:///tmp/new.lance".parse()?,
    schema,
    WriteOptions { ordering: Ordering::Unordered, ..WriteOptions::default() },
).await?;
writer.write(batch).await?;
let summary = writer.commit().await?;
```

Enable the facade's `sink-lance` feature, or depend on `open-arrow-sink-lance`
directly. Cloud backends are opt-in through the shared storage features below.

`WriteOptions.target_file_size` sets Lance's native `max_bytes_per_file` parameter.
`None` keeps the SDK default. It is a soft data-file target: buffered pages, groups,
and file footers can make files larger; Lance's row-count limits also still apply.

Writes feed a capacity-one queue into Lance's native streaming writer. Native
buffering is bounded by SDK defaults; this adapter never collects all input
batches. `write` means accepted by the queue; encoding errors may surface on a
later write or commit. Only `commit` sends a successful end-of-stream and allows
Lance to publish a new manifest version. `abort` stops the worker and removes the
reserved destination when `cleanup_on_error` is enabled. Dropping closes the stream with an error and never commits,
but can leave orphan files for manual cleanup. Commit failures/cancellation can
have an uncertain publication outcome; do not automatically retry.

This sink supports Create, Append, and versioned Overwrite on local or remote datasets.
Append requires the same schema; Overwrite retains historical dataset versions.
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

let mut writer = LanceDatasetSink::default().open(
    "file:///tmp/indexed.lance".parse()?,
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

## Quantized vector indexes

`LanceDatasetSink::with_vector_index_kind(VectorIndexKind::IvfPq)` selects IVF_PQ
with L2 distance for shared vector-index requests, matching the conversion service.
`IvfFlat` remains the default. PQ uses the largest divisor of the vector dimension
up to 16 as its subvector count. The IVF partition policy is shared with IVF_FLAT;
Lance validates training-data requirements and reports failures through
`IndexCommitError` after data publication. Deterministic names include the algorithm.

## Streaming native blobs

Blob-marked schemas require a single writer; blob bytes use the configured shared storage.
Mark a `LargeBinary` field with `open-arrow-sink:blob = true`. For each streamed
value, call `Writer::create_blob`, pass chunks to the returned `BlobWriter`, and
finish it to obtain a session-owned `BlobHandle`. Attach handles with
`Writer::write_with_blobs` and `BlobPlacement`; their cells must contain empty,
non-null binary placeholders. Null cells remain null. Ordinary materialized
binary values can still be written through `Writer::write`.

Streams append through the common `StorageWriter`, and batches carry native
Lance managed-blob descriptors. Streaming values use dedicated storage without
buffering the full payload to select inline or packed storage. Materialized binary
inputs remain supported through Lance's prepared-blob path. Handles from another
session and duplicate/null placements are rejected. Dropping an unfinished blob
poisons the session. Abort or a rejected commit removes a freshly reserved dataset
when `cleanup_on_error` is enabled; existing datasets are preserved.

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

## Writing and shared storage

Ordinary single-writer sessions use the same fragment writers and session
committer as `UnorderedWriteLayer::new(concurrency)`. Streaming blobs use a
specialized single writer with the same shared storage and HTTP request budget. Workers produce uncommitted fragments;
the committer publishes one data manifest and then builds the requested indexes.

Requests are `Url` values. Convert local paths with
`Url::from_directory_path(std::path::absolute(path)?)`. For custom credentials,
endpoints, or shared memory, pass a configured `Storage`:

```rust,ignore
use open_arrow_sink_lance::{LanceDatasetSink, Storage};
use open_arrow_sink_storage::opendal;

let operator = opendal::Operator::new(opendal::services::Memory::default())?;
let storage = Storage::new(operator, "memory:///warehouse/".parse()?)?;
let sink = LanceDatasetSink::new(storage.clone());
let url = storage.url("new-dataset/")?;
let mut writer = sink.open(url.clone(), schema, options).await?;
writer.write(batch).await?;
writer.commit().await?;
let dataset = sink.dataset_builder(url)?.load().await?;
```

Reads, writes, commits, and indexes share that operator. Enable `aws`, `gcp`, or
`azure` (facade: `lance-aws`, `lance-gcp`, `lance-azure`) for cloud URLs. Use an
explicit `Storage` for memory URLs so reads and writes share the same store.

Append requires the same effective schema, including compression metadata.
Overwrite publishes a new version, retaining historical versions, and can also
create a missing dataset. Abort waits for workers to stop and removes only a
freshly reserved destination when `cleanup_on_error` is enabled. Existing
datasets and files with an uncertain publication outcome are retained.

`DownloadBlobLayer` uses this streaming interface directly: network chunks are
appended to native blob files and only completed handles enter record batches.
A blob larger than the download pool's buffer budget is supported. The shared
budget applies to application-owned download chunks; Lance's internal I/O and
encoding buffers are separate. HTTP errors or cancellation poison the layer and
native session, and a rejected commit aborts the reserved dataset directory.

## Storage version

Set `WriteOptions.storage_version` to `Some("2.1".into())` to select Lance's native
data-file format version. `SinkCapability.supports_storage_version` is `true`.
Accepted values follow Lance 14: `legacy` / `0.1`, `2.0` / `0.3`, `2.1`, `2.2`,
`2.3`, `stable`, and `next`. Invalid values fail before destination I/O.
`None` keeps Lance's default: the latest stable format for a new dataset, or the
existing manifest's format for an existing dataset. An explicit version is passed
to Lance for create, append, and overwrite, including parallel and blob writers.
Appending with an explicit version affects the new files and preserves the manifest's
default version. This selects the file format, independently of the dataset's commit version.
