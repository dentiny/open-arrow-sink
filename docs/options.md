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
(for example, Append requires an existing destination).

## Ordering

`Ordering::PreserveInput` is the default. It requires preservation of input row
sequence in the backend's documented read order; it does not sort by column values.
Parallel backends can delegate `Sink::open` to the default
`ParallelSink::open_writer()`, which opens one writer and adapts its `WriteSession`
with `into_writer()`.
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

Lance accepts `Compression::Lz4Raw` as its native `lz4` policy for global,
per-column, and nested overrides. Native framing and codec selection still apply:
Miniblock uses LZ4, while some Fullzip per-value paths in Lance 14 substitute Zstd.

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
            .with_target_file_size(256 * 1024 * 1024),
    )
    .await?;
writer.write(batch).await?;
let summary = writer.commit().await?;
```

The unordered layer advertises `supports_blob_streaming: false`: its data writers
have no native streaming blob interface. Download URLs independently with
`storage::BlobDownloader`, then submit logical blob inputs. Native blob inputs
and `WriteOptions.blob_handle` configuration remain backend-controlled.

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
then calls the session committer once with all outputs. A finish failure returns
an error without aborting writers or publishing the session.
Abort waits for in-flight I/O and attempts to abort every writer, even if another
abort fails, then aborts the session. Drop never commits or performs async cleanup.

`Ordering::PreserveInput` is rejected before opening files. An outer
`OrderingLayer::new(Ordering::Unordered)` can explicitly enable reordering.

Publication guarantees belong to the backend. Parquet files become visible before
commit and have no atomic directory transaction. Transactional adapters can gather all outputs into one native data commit. Once publication begins, its outcome may be
uncertain on error or cancellation: the layer does not delete potentially committed
data. Failures return directly without automatic abort or deletion. Explicit
abort stops pending I/O and retains persisted output.

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
    .with_target_file_size(256 * 1024 * 1024);
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

## Blob downloader and memory limits

Every `LanceDatasetSink` owns a `storage::BlobDownloader` by default. Blob writes
always use it to materialize inputs before handing them to Lance: URI inputs in
`Ingest` mode are downloaded completely; existing bytes are admitted under the
same memory budget. `Reference` mode preserves external references without
copying their source. Ordinary Arrow strings stay strings. Install
`layer::DownloadBlobLayer::new(["image_url".to_owned()].into())` to explicitly
select top-level URL columns for blob ingestion. The layer marks selected text
fields; the backend downloads them through the same built-in component. Native
logical blob fields remain an alternative for callers already using SDK arrays. Lance selects inline, packed, or dedicated layout
from the complete payload size and `WriteOptions.blob_handle` thresholds.

Configure and share the built-in component when limits should span sinks:

```rust,no_run
use std::{num::NonZeroUsize, sync::Arc};
use open_arrow_sink::{lance::LanceDatasetSink, storage::BlobDownloader};

# fn example() -> Result<(), Box<dyn std::error::Error>> {
let downloader = Arc::new(BlobDownloader::new(
    NonZeroUsize::new(16).unwrap(),
    NonZeroUsize::new(1024 * 1024 * 1024).unwrap(),
)?.with_download_limits(
    NonZeroUsize::new(16 * 1024 * 1024).unwrap(), // payload limit per row
    NonZeroUsize::new(2 * 1024 * 1024).unwrap(),  // application copy chunk
)?);
let sink = LanceDatasetSink::default()
    .with_blob_downloader(downloader)
    .with_encoding_buffer_size(NonZeroUsize::new(32 * 1024 * 1024).unwrap());
// Write normal logical blob batches, including BlobArrayBuilder::push_uri.
# let _ = sink;
# Ok(())
# }
```

Three downloader limits work together:

- Download concurrency bounds active groups. Unknown-size URL rows and nested
  blobs use one group per row, reading selected sources sequentially. Known
  top-level in-memory payloads are batched up to 1024 rows while fitting one
  payload limit; an oversized row still fails independently. Groups run
  concurrently, preserving input order at handoff. Ready plus active groups are bounded together by both
  configured concurrency and the number of reservations the memory budget fits. Pending
  downloads continue being polled while the SDK's input channel is full, so
  sharing HTTP permits with destination uploads does not stall body consumption.
  All writers using the same downloader share admission.
- The HTTP storage's request limiter bounds requests through response-body EOF,
  failure, or cancellation. Its default equals download concurrency; install a
  shared `HttpConcurrencyLimitLayer` to share capacity with destination storage.
- `max_buffered_bytes` bounds shared application reservations. Each group
  atomically reserves `3 * payload_limit + chunk_limit`: complete payloads,
  two copies of assembly/SDK headroom, and scratch. All blob payloads in one row, including
  nested blobs, must fit the payload limit. Oversized rows fail. A download never
  waits while holding only part of its reservation. Payload reservations follow
  shared Arrow buffers through the native writer's bounded queue and release
  after consumption, cancellation, or error.

Without `with_download_limits`, each group reserves the shared budget divided
by concurrency. Scratch is at most 2 MiB and one third of
that reservation; one third of the remainder is the payload limit. `with_defaults()`
uses 64 download slots and an 8 GiB shared budget. No environment override or
process-wide singleton changes these limits. Cloned downloaders share budgets;
newly constructed sinks have independent default budgets unless configured to
share a downloader.

The downloader budget covers application-owned blob materialization and queued
payloads. Caller-owned input batches, Arrow metadata, OpenDAL/HTTP/TLS buffers,
and SDK encoding/output caches remain separate; this is not a process RSS cap.
`with_encoding_buffer_size` configures Lance's native column-data cache per
physical writer. Divide an encoding budget by writer count for parallel writes;
configure storage upload part size and HTTP concurrency for upload buffers.

HTTP(S) downloads preserve escaped paths and signed queries, support redirects,
and do not require HEAD or range support. Configure the default `OpenDalDownloader`
with retry, timeout, and transport policies, then attach it with `with_downloader`.
No retry or timeout is enabled by default.
`LanceDatasetSink::with_source_storage` supplies independent source credentials
to both the downloader and SDK. The destination's own namespace remains readable
through its configured operator, taking precedence over overlapping source roots. Error bodies are discarded before diagnostics
can collect them without a bound.

Implement `storage::Downloader::open(source, DownloadContext)` to supply custom
I/O as a stream of chunks. The shared `BlobDownloader` wrapper still applies
admission, chunk-size checks, payload limits, and queued-memory ownership.
Custom HTTP clients retain `context.http_requests().acquire().await` guards
through response-body EOF/drop; provider-owned buffers remain their responsibility.

The component also exposes `download(url)` and `download_all(&urls, metrics)` for
independent consumers, plus `begin`/`buffer` for admitted in-memory payloads.
Returned bytes and their clones retain the shared memory reservation. Downloads expose complete payloads; chunk streams belong to the `Downloader`
implementation boundary.
Dropping a pending write cancels its downloads and poisons the writer. Commit
then returns the error without publishing partial data or deleting persisted output. Lance accepts logical blob inputs; its former streaming-handle API is unsupported.


Download failures default to terminal (`DownloadFailurePolicy::Fail`). Use
`DownloadBlobLayer::with_failure_policy(DownloadFailurePolicy::SkipRow)` to omit
an entire row if any selected source fails, or `DownloadFailurePolicy::Null` to
replace only failed selected blobs with null. Null makes selected output fields
nullable. Recovery runs inside the backend's bounded blob materialization, before
SDK writes; destination errors, memory/chunk limits, and cancellation remain
terminal. Other blob fields retain their default behavior.

`layer.metrics().snapshot()` reports observed `blobs_failed`, unique
`rows_failed_blobs` per input batch, and `rows_skipped` after survivors are handed
to the SDK. Handles aggregate cloned layers and sessions. Input nulls, destination
errors, and budget failures do not count as failed sources. General `MetricsLayer`
row counters observe accepted input rows; the backend write summary counts output
rows after filtering.

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
`WriteOptions.io_metrics`; backends and the built-in downloader report I/O independently of
commit success. You can also supply your own handle without installing the layer.
Existing complete `WriteOptions` literals should add `io_metrics: None` or
`..Default::default()`.

`Storage` uses one OpenDAL `IoMetricsLayer` for byte observations across backends.
Lance and Parquet attach the session handle from `WriteOptions`; native adapters,
parallel fragments, metadata payloads, and index work use the same operator layer.
The old `IoMetricsSource`, `IoMetricsRegistration`, and `IoMetrics::track` APIs are
removed; byte owners retain cloned observers directly. The downloader records
consumed source chunks, including transfers before failure or cancellation.

For Iceberg, configure the catalog's `OpenDalStorageFactory` with
`storage.with_io_metrics(metrics.clone())`. Its FileIO belongs to the caller's
catalog, so `WriteOptions.io_metrics` cannot replace that FileIO. The configured
handle covers data, manifest, and metadata I/O, including catalog work and readback;
use a separate catalog/storage observer when isolation is needed. Custom sinks
that bypass instrumented Storage still contribute no bytes.

Counters record bytes returned by successful reads and accepted by successful
writes, including transfers before a later failure, cancellation, abort, or drop.
Buffered writes contribute when OpenDAL accepts them; acceptance does not imply
publication or a successful close. Streaming readers may fetch more bytes than a
consumer ultimately uses. Metadata operations, server-side copies, HTTP framing,
and internal retries are excluded. These are payload observations, not network
totals or bytes inferred from Arrow batches. Install the layer outside retry layers;
`Storage` does this when exporting an operator. Snapshots do not reset counters.

The layer forwards schema, capabilities, native blob APIs, and parallel sessions.
Install one observation layer at the boundary you want to measure. The built-in
blob downloader records source reads using the writer's `IoMetrics` handle, so
its traffic is included regardless of URL-selection layers. Shared source observers
are deduplicated against the downloader handle, preserving independent observers. Nested observers receive
backend measurements independently; reusing the same handle does not duplicate
byte observations. Placing MetricsLayer inside or outside UnorderedWriteLayer
still counts one successful session commit.

## Upload buffering and concurrency

`WriteOptions.upload: UploadConfig` groups session upload overrides. Its
`part_size: Option<NonZeroUsize>` sets upload part size in bytes, and
`concurrency: Option<NonZeroUsize>` bounds concurrent upload requests
per writer. Parquet and Lance support these session overrides, including parallel
writers and Lance native multipart uploads. Check `SinkCapability.supports_upload_options`.
Iceberg rejects explicit overrides because its FileIO is supplied by the catalog;
configure its `Storage` before constructing `OpenDalStorageFactory` instead.

```rust
use std::num::NonZeroUsize;
use open_arrow_sink::{Ordering, UploadConfig, WriteOptions};

let options = WriteOptions::default()
    .with_ordering(Ordering::Unordered)
    .with_upload(
        UploadConfig::default()
            .with_part_size(NonZeroUsize::new(16 * 1024 * 1024).unwrap())
            .with_concurrency(NonZeroUsize::new(4).unwrap()),
    );
```

Unset fields retain the configured `Storage` value (otherwise 4 MiB and four concurrent
requests per writer). Overrides affect only the current session. OpenDAL enforces
backend part-size limits. These options differ from `target_file_size`, which
controls file rolling, and from the shared aggregate HTTP request budget. Memory
use grows with part size, upload concurrency, and the number of writers.
