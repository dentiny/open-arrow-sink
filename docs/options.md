# Write options and operational contracts

See [usage](usage.md) for complete applications, [architecture](architecture.md) for ownership,
and [layers](../layer/README.md) for composition. This reference describes the shared options;
backend-specific limits are documented in the [support matrix](support.md).

## Destination write modes

`WriteOptions.mode` accepts `WriteMode::Create`, `Append`, or `Overwrite`.
Create requires a new destination; Append requires an existing one. Overwrite
replaces all rows, not just affected partitions, and may create a missing
destination. Atomicity and cleanup are backend-specific; see the
[support matrix](support.md#visibility-and-failure).

The default is Create for every sink. Iceberg supports only Append, so callers
must explicitly set `mode: WriteMode::Append`. `SinkCapability.supported_modes`
lists implemented modes, but destination-dependent restrictions still apply
(for example, Parquet Overwrite requires a filesystem URL/operator).

## Ordering

`Ordering::PreserveInput` is the default. It requires preservation of input row
sequence in the backend's documented read order; it does not sort by column values.
Single-writer backends can adapt their `WriteSession` with `into_writer()`.
This shares write-failure, cancellation, finish, abort, and publication handling
across sinks; backend data writers only produce output, and committers publish it.

A backend declares its guarantee through `SinkCapability.ordering`: either
`Ordering::PreserveInput` or `Ordering::Unordered` (no input-order guarantee). It must reject a preserve-input
request if it cannot provide that guarantee. Writing
files sequentially does not by itself guarantee directory or table scan order.

`Ordering::Unordered` allows reordering rows and batches without losing or
duplicating data. It neither shuffles data nor starts concurrent execution.

```rust,ignore
use open_arrow_sink::{Ordering, SinkExt, layer::OrderingLayer};

// `backend` is an application-defined implementation of Sink.
let sink = backend.layer(OrderingLayer::new(Ordering::Unordered));
```

The ordering layer overrides the session option and checks the inner capability
before opening it. Option layers validate only their own constraints; the backend
validates the effective options after all inner overrides. Install one ordering
layer per sink. Layers wrap without I/O; the last installed layer is outermost.

## Compression

`WriteOptions.compression` selects a global default and/or per-column overrides.
Empty `CompressionOptions::default()` preserves backend defaults; explicit
`Compression::Uncompressed` disables compression. Column overrides take
precedence over the entire global compression setting, including its level.
`Compression::Zstd { level: Some(9) }` selects an explicit Zstandard level;
`level: None` preserves the backend's default level. Other codecs use their
backend defaults. Levels use `i32` so backends that support negative Zstandard
levels can expose them. Each backend validates its supported level range before
destination I/O and must reject unsupported values without silently clamping or
ignoring them.

`SinkCapability.compression` advertises supported codecs independently through
`CompressionCapability.global` and `.per_column`; an empty slice means that
scope is unsupported. Capabilities use `CompressionCodec` identifiers, independent
of the requested level. The shared validator rejects unsupported scopes and codecs
before destination I/O, including requests to disable compression.

Parquet compression dependencies are opt-in: enable facade features
`parquet-snappy`, `parquet-gzip`, `parquet-zstd`, or `parquet-lz4` for the codecs
you need. `sink-parquet` alone supports uncompressed output.

Backend support is declared by each sink; consult its README before selecting
a scope or codec. The shared interface does not promise that every sink supports
every codec.

```rust,ignore
use open_arrow_sink::{Compression, CompressionOptions, SinkExt, layer::CompressionLayer};

let sink = backend.layer(CompressionLayer::new(CompressionOptions {
    global: Some(Compression::Zstd { level: Some(9) }),
    columns: [("embedding".into(), Compression::Uncompressed)].into(),
    ..Default::default()
}));
```

The layer replaces the entire session compression policy and forwards the inner
capability unchanged. Install one compression layer per sink. The same options
can be supplied directly in `WriteOptions`, without a layer.
Column keys are exact, unique top-level Arrow field names: dots are literal,
and a nested field override applies to all its descendant physical columns.
Unknown or ambiguous names are rejected before opening the destination.
Backends retain the session policy across any file rolling.

`CompressionOptions` also contains structural encoding policy, independently of
its codec policy. `structural_encoding` selects a global `Miniblock` or `Fullzip`
layout; `column_encodings` overrides that choice for exact, unique top-level
columns. Column codec overrides do not reset structural policy, and structural
overrides do not reset codecs or their levels. Both policies apply recursively to
nested descendants. Empty policies preserve existing schema metadata and backend
defaults. `CompressionLayer` replaces all four fields together.

`CompressionCapability.structural_encodings` advertises supported layouts for
both scopes. Lance supports both; Parquet and Iceberg reject nonempty structural
policy before destination I/O. Lance rejects structural overrides on native blob
columns, which use their own storage layout. Matching effective structural schema
metadata is required when appending to an existing dataset.

## Unordered concurrent writes

`UnorderedWriteLayer` is available by default. Unlike `OrderingLayer`,
this layer pre-opens a fixed number of writers and drives their I/O concurrently:

```rust,ignore
use open_arrow_sink::{Ordering, Sink, SinkExt, WriteOptions, layer::UnorderedWriteLayer};
use std::num::NonZeroUsize;

// `backend` implements ParallelSink.
let sink = backend.layer(UnorderedWriteLayer::new(NonZeroUsize::new(4).unwrap()));
let mut writer = sink
    .open(
        directory,
        schema,
        WriteOptions::default()
            .with_ordering(Ordering::Unordered)
            .with_target_file_size(Some(256 * 1024 * 1024)),
    )
    .await?;
writer.write(batch).await?;
let summary = writer.commit().await?;
```

The unordered layer advertises `supports_blob_streaming: false`: its data writers
have no native streaming blob interface. An outer `DownloadBlobLayer` selecting
URL columns is rejected before opening writers. Native logical blob inputs and
`WriteOptions.blob_handle` configuration remain backend-controlled.

The inner sink implements `ParallelSink`. It reserves a single destination and
returns a `WriteSession` containing independent `DataWriter`s and one
`SessionCommitter`. The layer never interprets backend output metadata, file names,
partition specs, or manifests. Backend adapters supply their own publication and
cleanup behavior; no source-library metadata is needed.

At most one batch per writer is in flight. When all writers are busy, `write`
waits for one to finish. An available writer accepts the next nonempty batch;
there is no round-robin or file-order guarantee. Empty batches are validated and
otherwise ignored. The bound is in batches, not bytes. Each backend also retains
its own encoding buffers.

The layer spawns no tasks: it polls data-writer futures during `write`, `commit`,
or `abort`. Parquet and Iceberg encoding/compression consequently runs in the
caller task, so multiple writers overlap I/O but do not add CPU encoding threads.
A multithreaded runtime does not change this unless the application or backend
schedules separate tasks. More writers also multiply backend encoding buffers;
tune concurrency against the actual CPU or upload bottleneck. Lance schedules
its native writes separately and uses bounded channels. For application-managed
CPU scheduling and its writer ownership requirements, see
[concurrent writes](../layer/README.md#concurrent-writes).

A successful `write` means accepted; an asynchronous error may surface
in a later call. An error or cancellation of `write` makes the session terminal;
abort or drop it. Commit drains accepted writes, finishes every data writer,
then calls the session committer once with all outputs. A finish failure aborts
all writers, including those that already finished, without publishing the session.
Abort waits for in-flight I/O and attempts to abort every writer, even if another
abort fails, then aborts the session. Drop never commits or performs async cleanup.

`Ordering::PreserveInput` is rejected before opening files. An outer
`OrderingLayer::new(Ordering::Unordered)` can explicitly enable reordering.

Publication guarantees belong to the backend. Parquet files become visible before
commit and have no atomic directory transaction. Transactional adapters can gather all outputs into one native data commit. Once publication begins, its outcome may be
uncertain on error or cancellation: the layer does not delete potentially committed
data. The session committer owns publication-failure handling. Before publication,
cleanup honors `cleanup_on_error` and removes only session-owned output.

## Partitioning

`WriteOptions.partition: Some(PartitionOptions::default())` enables routing using
an existing destination's partition spec. `None` disables routing (the default).
Only the Iceberg sink currently supports this option, for `Identity`, `Year`,
`Month`, `Day`, `Hour`, `Bucket`, `Truncate`, and `Void` transforms; Parquet and
Lance reject it before creating output. Backends declare supported transform categories
through `SinkCapability.supported_partitions`; an empty list means partitioning
is unsupported.
The requested `PartitionOptions.partition_type` (default `Identity`) must be
in that list and match every partition field in the destination spec. No
partition layer is required. Bucket counts and truncate widths come from the
destination spec. All-void specs write a single logical partition with null keys.

The Iceberg adapter uses the SDK's fanout writer behavior. See the
[Iceberg sink](../sink/iceberg/README.md) for configuration and commit semantics.

## Storage version

`WriteOptions.storage_version: Option<String>` selects the backend data-file format
version. `None` preserves the backend default. `SinkCapability.supports_storage_version`
advertises support: currently only Lance returns `true`. Parquet and Iceberg reject
explicit versions with `Error::Unsupported` before destination I/O.

## File size

The shared `WriteOptions.target_file_size` option specifies a positive byte count:

```rust
use open_arrow_sink::{Ordering, WriteOptions};

let options = WriteOptions::default()
    .with_ordering(Ordering::Unordered)
    .with_target_file_size(Some(256 * 1024 * 1024));
```

This is a soft data-file target, not a strict maximum or a total-output limit.
Files can exceed it because batches, pages, row groups, and footers are not split
at exact byte boundaries. `None` preserves the backend default, and zero is
rejected before opening a destination. Backend-specific rolling is implemented
separately; see each sink's README for support and granularity. Other backend
limits may also cause rolling before the target is reached.

## Index construction

Index requests are backend-independent `IndexSpec` and `IndexType` values in
`WriteOptions.indexes`. The request list is empty by default.
`SinkCapability.supported_indexes` lists the supported `IndexType` values. Each
requested type must appear in that list. Lance supports Scalar, Vector, Text,
and BloomFilter; Parquet and Iceberg expose empty lists and reject index requests
before opening a writer or accessing the destination.
General layers forward this capability and the options to their inner sink.

```rust
use open_arrow_sink::{IndexSpec, IndexType, Ordering, WriteOptions};

let options = WriteOptions::default()
    .with_ordering(Ordering::Unordered)
    .with_indexes(vec![
        IndexSpec::new("id", IndexType::Scalar),
        IndexSpec::new("embedding", IndexType::Vector),
    ]);
```

Backends choose the concrete implementation of each logical index type and validate
column types before writing. Lance maps the four kinds to BTree, IVF_FLAT with L2,
Inverted, and BloomFilter, then constructs them after committing the dataset's data.
See the [Lance documentation](../sink/lance/README.md) for construction failure semantics.
`WriteOptions` owns its index list and implements `Clone`; clone options when
reusing them across sessions.

## Streaming download pool

`layer::DownloadPool` re-exports the common storage downloader, using its HTTP
concurrency limiter. Construct a pool with
`DownloadPool::new(concurrency, max_buffered_bytes)` and share its `Arc` across
writers. Every pool shares one process-wide request limiter. The first pool reads
`OPEN_ARROW_SINK_DOWNLOAD_CONCURRENCY`, falling back to its `concurrency` argument
when unset, non-Unicode, invalid, or zero. Later pools reuse that limit, ignoring
their `concurrency` arguments. Values above semaphore capacity return an error
without initializing the limiter. `DownloadPool::global()` uses a fallback of 64
and an 8 GiB transfer-buffer budget. Transfer-buffer budgets remain per pool;
creating a new pool does not add independent request slots.

`pool.stream(url)` returns a pull-based stream of owned `BlobChunk` values.
Consume each chunk (for example with `BlobWriter::write`) before requesting more.
The pool reserves buffer capacity before allocation and reading; the reservation
travels with the chunk until the consumer drops it. Retaining a chunk keeps its
budget charged even if the source stream is cancelled. Exhausting the shared
budget pauses readers, including readers from other writers. Whole blobs are
never collected by the pool and may exceed its budget.

The budget covers application-owned transfer buffers. OpenDAL/HTTP/TLS read buffers,
Lance encoding/output buffers, input batches, and descriptor metadata are separate;
it is not a process RSS limit. Error response bodies are discarded before OpenDAL
classifies the status to avoid collecting unbounded diagnostic payloads.

HTTP(S), escaped paths, signed queries, and redirects are supported. File and enabled cloud URLs are also supported; configure their credentials and policies with `DownloadPool::with_source_storage`. No timeout or
retry policy is enabled by default. Apply OpenDAL policies with
`DownloadPool::with_layer`, adding `TimeoutLayer` before `RetryLayer` so every
attempt gets its own I/O deadline. Dropping a stream cancels pending work and
releases its request slot; dropping its yielded chunks releases their byte budget.

## Download URL columns into native blobs

Compose `DownloadBlobLayer` with a sink advertising `SinkCapability.supports_blob_streaming` (currently
`LanceDatasetSink`). Select top-level `Utf8`, `LargeUtf8`, or `Utf8View` URL
columns by name. The layer passes budgeted chunks directly from OpenDAL into the
sink's native blob writer; it never collects a complete blob or a batch of payloads.
Completed, session-owned descriptors are attached to the corresponding rows.
The download layer consumes native streams internally and advertises
`supports_blob_streaming: false` to callers. Select all URL columns in one layer;
nesting download layers is rejected before opening the destination.
`supports_blob_handle` separately governs native logical blob inputs and
`WriteOptions.blob_handle` configuration.

```rust,no_run
use std::{num::NonZeroUsize, sync::Arc};
use open_arrow_sink::{SinkExt, lance::LanceDatasetSink, layer::{DownloadBlobLayer, DownloadPool}};

# fn example() -> open_arrow_sink::Result<()> {
let pool = Arc::new(DownloadPool::new(
    NonZeroUsize::new(16).unwrap(),
    NonZeroUsize::new(64 * 1024 * 1024).unwrap(),
)?);
let sink = LanceDatasetSink::default().layer(
    DownloadBlobLayer::new(["image_url".to_owned()].into_iter().collect())
        .with_download_pool(pool),
);
# Ok(())
# }
```

`DownloadBlobLayer` is always available. Enable `sink-lance` for this example. Input batch
boundaries, row order, nulls, and metadata are preserved by the layer; the final
sink's ordering guarantee still applies. There is no per-blob size limit or
separate output batch size. Payloads larger than the shared buffer budget stream
through in chunks. Streamed Lance blobs use native dedicated storage, and default
scans return descriptors; use `blob_handling(BlobHandling::AllBinary)` to read bytes.

The input writer schema retains URL columns. The inner writer schema has
blob-marked `LargeBinary` fields with empty placeholders; payloads are carried by
native handles, not those arrays. Sinks without streaming blob support are rejected
when opening the layer. Blob ingestion currently uses single-writer sessions;
the underlying sink controls the final scan ordering.

A failed or cancelled write poisons the session and cancels remaining downloads;
commit then aborts the inner writer. Earlier batches may already have been accepted.
Dropping chunks releases the shared byte budget, and native blob cleanup follows
the writer's abort/drop guarantees.

### Blob handle configuration

`WriteOptions.blob_handle: Option<BlobHandleConfig>` configures each write session.
The configuration follows Lance's `BlobFieldOptions`: `inline_size_threshold`
(inline to packed) and `dedicated_size_threshold` (packed to dedicated), plus
`external_blob_mode` (`Ingest` by default or explicit `Reference`). Thresholds
apply to top-level native blob fields; omitted thresholds preserve field metadata
or Lance defaults. `blob_handle: None` still uses `Ingest`. Lance advertises
`SinkCapability.supports_blob_handle`; Parquet and Iceberg reject explicit blob
configuration before destination I/O. Native schema and parallel-session restrictions
still apply. See the [Lance blob options](../sink/lance/README.md#external-blob-ingestion).

### Existing indexes

`IndexSpec::new` defaults to `ExistingIndexPolicy::Error`. Use
`.with_existing_policy(ExistingIndexPolicy::Keep)` to retain an existing named
index or `Rebuild` to recreate it over all committed rows. Lance preflights
Append name conflicts for Error before accepting data. Index work still occurs
after publication; training or storage failures return `IndexCommitError` and
leave committed data visible. Keep can leave appended fragments unindexed.

`IndexSpec` gains an `existing` field; constructors preserve the default behavior.

### Nested Lance compression overrides

`CompressionOptions.nested_columns` uses literal path segments, for example
`[(vec!["messages".into(), "text".into()], Compression::Zstd { level: Some(6) })].into()`.
Struct children are named directly; list containers are transparent (omit `item`).
Dots inside a segment are literal field names. Paths must uniquely identify a
nested field and cannot enter native blob internals. Lance applies global policy,
then top-level overrides, then nested overrides from ancestors to descendants.
A nested codec override preserves independently selected structural encoding.
Parquet and Iceberg reject this scope before opening the destination.

`CompressionOptions` gains `nested_columns`; complete literals should use
`..Default::default()`. `CompressionCapability.nested_columns` advertises support.

## Observing sessions with MetricsLayer

Compose `sink.layer(MetricsLayer::default())` for backend-independent observation.
Keep `let metrics = layer.metrics()` before moving the layer into the sink and
call `metrics.snapshot()` at any time. `MetricsLayer::new(WriteMetrics::default())`
also accepts a shared handle. Counters aggregate all sessions using that handle.

The layer records opened/committed/aborted sessions, accepted batches and rows,
committed rows, failed/cancelled operations, and write/commit durations. Accepted
rows include buffered data that may later fail; committed rows count only successful
commit returns, not uncertain or partially successful native publication. Durations
are accumulated wall time of operations and can overlap with parallel writers.

Payload-byte observations are separate from `WriteSummary`, which only returns
`rows_written`. `MetricsLayer` passes a shared `IoMetrics` handle through
`WriteOptions.io_metrics`; backends and download layers report I/O independently of
commit success. You can also supply your own handle without installing the layer.
Existing complete `WriteOptions` literals should add `io_metrics: None` or
`..Default::default()`.

Lance registers a fresh counter for each session, covering native reads, parallel
fragment writes, manifest/index work, and successfully closed streamed blobs.
Counters are visible while sessions are active and retained after commit, failure,
cancellation, abort, or drop. Active sources are released after their I/O owners
are dropped, retaining final totals. DownloadBlobLayer records each source chunk
as it is consumed, including chunks consumed before a later error or cancellation.
Failed I/O requests and unfinished streamed-blob writes are not counted as
successful payload writes. These are measured logical payloads, not HTTP headers,
network retries, process memory, or bytes inferred from Arrow batches.

Byte counters start at zero and sum only instrumented I/O. Parquet, Iceberg, and
custom sinks without instrumentation contribute no bytes; a mixed-backend handle
therefore reports measured traffic, not a complete total for every backend.
Snapshotting does not reset counters.

The layer forwards schema, capabilities, native blob APIs, and parallel sessions.
Install one observation layer at the boundary you want to measure. Placing it outside
DownloadBlobLayer includes downloaded source bytes; placing it inside observes only
the native sink's I/O. Nested observers receive backend measurements independently;
reusing the same handle does not duplicate byte observations. Placing MetricsLayer
inside or outside UnorderedWriteLayer still counts one successful session commit.
