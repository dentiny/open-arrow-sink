# Lance dataset sink

`LanceDatasetSink` writes a Lance dataset from Arrow batches at a local or
remote URL. It requires a Tokio runtime. Missing parents are created.
Create rejects existing destinations; Append requires an existing dataset.
Keep exclusive ownership of the destination until the session ends, including
index construction.

```rust,ignore
use open_arrow_sink::{Ordering, Sink, WriteOptions, lance::LanceDatasetSink};

let mut writer = LanceDatasetSink::default()
    .open(
        "file:///tmp/new.lance".parse()?,
        schema,
        WriteOptions::default().with_ordering(Ordering::Unordered),
    )
    .await?;
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
Lance to publish a new manifest version. `abort` stops the worker and retains
persisted output. Dropping closes the stream
with an error and never commits,
but can leave orphan files for manual cleanup. Commit failures/cancellation can
have an uncertain publication outcome; do not automatically retry.

This sink supports Create, Append, and versioned Overwrite on local or remote datasets.
Create checks that the destination prefix does not exist without creating marker
files. Callers must exclude concurrent writes or directory replacement during a
session. After a process crash, uncommitted output may require manual cleanup
before another Create.
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
use open_arrow_sink::{
    IndexSpec, IndexType, Ordering, Sink, WriteOptions, lance::LanceDatasetSink,
};

let mut writer = LanceDatasetSink::default()
    .open(
        "file:///tmp/indexed.lance".parse()?,
        schema,
        WriteOptions::default()
            .with_ordering(Ordering::Unordered)
            .with_indexes(vec![
                IndexSpec::new("id", IndexType::Scalar),
                IndexSpec::new("embedding", IndexType::Vector),
            ]),
    )
    .await?;
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
builds requested indexes one at a time to bound peak native build memory. Each task builds and commits its index
through Lance, potentially publishing another dataset version. Names are deterministic. `IndexSpec::with_existing_policy` selects `ExistingIndexPolicy::Error` (the default), `Keep`, or `Rebuild`. Append checks default-policy name conflicts at open before accepting data. Keep retains the existing index; new fragments can remain unindexed and native scans search uncovered rows. Rebuild replaces the named index over the committed dataset. All started tasks are awaited, including
when one fails, and the original dataset handle is refreshed after they finish.
The first error in request order is returned after all requests have been attempted. Vector builds share one lazy row-count result for the committed data snapshot; Keep requests that retain an existing index do not count rows. Successfully built
indexes and committed data remain visible after an error.
`Error::IndexCommit` contains an `IndexCommitError` identifying the failed index type
and column, or a task failure if a worker panics. Its `summary` retains the
committed row count even when indexing fails; retry index construction rather
than rewriting already committed rows. Dropping the commit future aborts
the task set; native publication may still have an uncertain outcome.

A runnable index construction example (destination must not exist):

```sh
cargo run -p open-arrow-sink-lance --example write_indexed -- /tmp/indexed-lance
```

## Materialized blob inputs

Ordinary Arrow text columns remain text, even when values look like URLs. Use
`layer::DownloadBlobLayer` to select URL columns for ingestion, native logical
blob fields (`BlobArrayBuilder::push_bytes` / `push_uri`), or mark a top-level `LargeBinary` field with `open-arrow-sink:blob = true`. Every
blob goes through the built-in downloader's admission/memory budget, then the
SDK selects its physical layout. Single and parallel writers use this same path.
Nulls and empty blobs remain distinct. Prepared descriptors and the old
`create_blob` / `write_with_blobs` streaming-handle API are unsupported by Lance.

## External blob ingestion

Lance advertises `SinkCapability.supports_blob_handle`. Configure each session
through `WriteOptions.blob_handle`; the storage thresholds match Lance `BlobFieldOptions`:

```rust
use open_arrow_sink_core::{BlobHandleConfig, ExternalBlobMode, Ordering, WriteOptions};
use std::num::NonZeroUsize;

let options = WriteOptions::default()
    .with_ordering(Ordering::Unordered)
    .with_blob_handle(BlobHandleConfig {
        external_blob_mode: ExternalBlobMode::Ingest,
        inline_size_threshold: Some(16 * 1024),
        dedicated_size_threshold: NonZeroUsize::new(2 * 1024 * 1024),
    });
```

Native logical blob source URIs are copied into destination-owned storage with
`Ingest`. The two layout options are payload-size boundaries in bytes:

- `inline_size_threshold: Option<usize>`: maximum size kept inline before using packed storage.
- `dedicated_size_threshold: Option<NonZeroUsize>`: maximum size kept packed before using dedicated storage.

These options map through native `BlobFieldOptions` to top-level blob field metadata.
Explicit values override existing field thresholds; `None` preserves field metadata
or Lance defaults (64 KiB inline, 4 MiB dedicated). Values strictly above each
threshold move to the next storage class. Dedicated selection takes precedence.
They control native logical blob writes, including ingested URIs.
The default is `ExternalBlobMode::Ingest`, including when `blob_handle` is `None`
or `Some(BlobHandleConfig::default())`. Choose `Reference` explicitly to retain external URIs.
This setting applies to native logical blob inputs in single and parallel writes;
downloaded materialized bytes use the same SDK layout selection.

For a runnable parallel ingestion workflow, use:

```sh
cargo run --locked -p open-arrow-sink-lance --example parallel_native_blobs
```

The self-contained example sends a batch to each of eight physical writers,
mixes materialized bytes with native source URIs, and commits one data manifest.
It verifies inline, packed, and dedicated blob readback after removing the source
objects. `with_source_storage` routes the independent source operator; use the
same mechanism for caller-configured S3 credentials and endpoints. In an ordinary
pipeline, native logical blob batches can also be dispatched through
`UnorderedWriteLayer`; actual writer utilization depends on available batches.

Materialized
blob-marked `LargeBinary` inputs also work with parallel writers: the adapter
converts them to logical blobs and the SDK chooses their layout.

## Compression

Supports global, per-column, and nested-column `Compression::Zstd { level }`,
`Compression::Lz4Raw`, and `Compression::Uncompressed`, directly in
`WriteOptions.compression` or through `CompressionLayer`. Zstandard levels 0 through 22 are supported; `None` uses
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
general-purpose compression. Structural encoding is independently controlled by
`CompressionOptions.structural_encoding` and `.column_encodings`: `Miniblock`
uses a block-oriented layout, while `Fullzip` uses a per-value layout.
The requested codec is passed to Lance for either layout. Column overrides replace only the corresponding
structural default, and both policies propagate to nested descendants.
Unspecified structural settings preserve native/schema defaults. Native blob
fields own their layout and reject explicit structural settings before I/O.
Append compares the effective metadata, so use the same policies as at creation.
`writer.schema()` and input batch validation use the caller's original schema.
Batches reuse their existing value buffers with the internal schema, and readback
includes the persisted encoding metadata. These settings survive file rolling.

`Lz4Raw` maps to Lance's native `lz4` policy. Native encoding determines the
on-disk framing; this is not a Parquet-compatible raw LZ4 stream. Miniblock
encoding uses LZ4, while some Fullzip per-value paths in Lance 14 substitute
Zstd. The adapter preserves this native behavior and the requested `lz4` schema
metadata. Snappy and Gzip are unsupported by Lance.

```rust
use open_arrow_sink_core::{Compression, CompressionOptions, StructuralEncoding};

let compression = CompressionOptions {
    global: Some(Compression::Zstd { level: Some(9) }),
    columns: [("raw_payload".into(), Compression::Uncompressed)].into(),
    structural_encoding: Some(StructuralEncoding::Fullzip),
    column_encodings: [("id".into(), StructuralEncoding::Miniblock)].into(),
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
use open_arrow_sink_core::Sink;
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
create a missing dataset. Explicit abort waits for workers to stop and retains
persisted output. Write and commit failures return directly without automatic
abort, deletion, or rollback.

Use `LanceDatasetSink::with_encoding_buffer_size(NonZeroUsize)` to set the SDK's
total column-data cache per physical writer. This is separate from downloaded
payloads and upload buffers; account for the writer count when budgeting memory.

The sink always owns a `storage::BlobDownloader`. Logical blob URI inputs are
downloaded automatically; materialized bytes use the same admission and memory
budget. Configure it with `with_blob_downloader(Arc<BlobDownloader>)`. Complete
payloads use SDK layout selection. Install a custom `Downloader` implementation
through `BlobDownloader::with_downloader` while keeping the framework budgets.

## Storage version

Set `WriteOptions.storage_version` to `Some("2.2".into())` to select Lance's native
data-file format version. `SinkCapability.supports_storage_version` is `true`.
Only `2.2` and `2.3` are supported. Older versions and aliases such as `stable`
and `next` are rejected before destination I/O. Existing datasets must also use
format `2.2` or `2.3`.
`None` keeps Lance's default: the latest stable format for a new dataset, or the
existing manifest's format for an existing dataset. An explicit version is passed
to Lance for create, append, and overwrite, including parallel and blob writers.
Appending with an explicit version affects the new files and preserves the manifest's
default version. This selects the file format, independently of the dataset's commit version.

## Configuration adaptation

The Lance `write_options` adapter translates shared configuration into native
`WriteParams` and encoding metadata. Both writer paths prepare one
`write_options::LanceSchemas` containing the original input schema, the logical
schema for dataset validation, and the encoded schema for native writes.
Compression is applied first; logical blob fields and encoded descriptors are then
derived together with the same metadata and explicit layout overrides.
`LanceDestination` binds
the resulting parameters to its session and commit handler; blob runtime code
owns handles, placements, and cleanup.

## Upload buffering

Configure `Storage::with_upload_part_size(NonZeroUsize)` before constructing the
sink to control the native data-file and sidecar multipart upload buffers, as
well as SDK-managed blob uploads. For example, select a 16 MiB part
size to tune buffering for your workload. Uploads default to 4 MiB parts and four
concurrent requests per writer; use `Storage::with_upload_concurrency` to increase
parallelism within your memory budget. This shared default replaces the native
adapter's previous implicit concurrency of eight. Conditional manifest writes retain
the ordinary OpenDAL adapter; multipart parts preserve their submission order.

Session upload buffering can be tuned with `WriteOptions.upload.part_size` and
`WriteOptions.upload.concurrency` (positive values represented by `NonZeroUsize`).
These override Storage settings for the session; unset values preserve defaults.
See [upload options](../../docs/options.md#upload-buffering-and-concurrency).

## Related documentation

[Usage](../../docs/usage.md) · [Architecture](../../docs/architecture.md) ·
[Support matrix](../../docs/support.md) · [Write layers](../../layer/README.md) ·
[Extend a sink](../../docs/extending-sinks.md)

### Independent blob sources

Use `LanceDatasetSink::with_source_storage(storage)` for native blob sources
requiring custom credentials, endpoints, or operators. The destination store
always takes precedence inside its own namespace; sources use the most specific
configured root. Destination credentials are never reused for unrelated sources.
Unregistered sources use URL-configured storage cached by `StorageResolver`.
Lance’s `Provider` applies destination precedence and adapts the selected storage
to Lance.

Configure sources once and pass the same `Arc<StorageResolver>` to the download
pool and native Lance sink. Both paths then use the same source operators,
credentials, and URL-derived cache:

```rust
use std::sync::Arc;
use open_arrow_sink_lance::LanceDatasetSink;
use open_arrow_sink_storage::{BlobDownloader, Storage, StorageResolver, opendal};

let source = Storage::new(
    opendal::Operator::new(opendal::services::Memory::default())?,
    "memory://sources/".parse()?,
)?;
let sources = Arc::new(StorageResolver::default().with_storage(source));
let pool = Arc::new(BlobDownloader::with_defaults()?.with_source_resolver(sources.clone()));
let sink = LanceDatasetSink::default().with_source_resolver(sources)
    .with_blob_downloader(pool);
// Blob writes always use the configured downloader.
# let _ = sink;
# Ok::<(), Box<dyn std::error::Error>>(())
```

`with_source_resolver` replaces prior source registrations. Calling
`with_source_storage` afterwards adds a store only to that consumer's resolver;
configure all shared stores before wrapping the resolver in `Arc`.
HTTP policy and transport configuration live on `OpenDalDownloader`, or a custom
implementation of the `Downloader` trait.

URL-based resolution supports enabled OpenDAL services. Enable the corresponding
service or register a builder with `opendal::OperatorRegistry` before opening the
sink or constructing a dataset builder; Lance snapshots the available schemes.

`BlobDownloader::with_source_storage` configures non-HTTP download sources.
The default `OpenDalDownloader` preserves signed query
strings. Configure shared downloader limits on the sink; it downloads URI inputs and
passes complete payloads to the SDK automatically. Plain text URL columns require
explicit selection with `DownloadBlobLayer`; native logical blob inputs do not. See the
[download contracts](../../docs/options.md#blob-downloader-and-memory-limits).
