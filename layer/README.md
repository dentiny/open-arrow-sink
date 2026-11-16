# Write layers

A write layer wraps a `Sink` without I/O and preserves its typed request.
Import `SinkExt` to use `.layer(...)`. All built-in layers are always available
through `open_arrow_sink::layer`; no facade feature is needed for the layer API.
Enable a backend feature to construct that backend.

| Layer | Changes | Required inner contract |
| --- | --- | --- |
| `OrderingLayer` | Overrides `WriteOptions.ordering`; does not sort or start workers | `Sink`; also forwards `ParallelSink` when implemented |
| `CompressionLayer` | Replaces codec and structural encoding policies, including column overrides | `Sink`; also forwards `ParallelSink` when implemented |
| `UnorderedWriteLayer` | Dispatches whole batches to a fixed set of independent data writers | `ParallelSink` |
| `DownloadBlobLayer` | Streams selected URL columns into native managed blobs | `Sink` advertising `supports_blob_streaming`; currently Lance single-writer sessions |

## Order of composition

The last installed layer is outermost. Operations in
`sink.layer(a).layer(b)` reach b → a → sink. Option changes must happen before a
layer validates/opens the inner session. Install one ordering and one compression
layer per stack to keep precedence clear.

```rust,ignore
use std::num::NonZeroUsize;
use open_arrow_sink::{Ordering, SinkExt, parquet::ParquetDirectorySink,
    layer::{OrderingLayer, UnorderedWriteLayer}};

let sink = ParquetDirectorySink::default()
    .layer(UnorderedWriteLayer::new(NonZeroUsize::new(4).unwrap()))
    .layer(OrderingLayer::new(Ordering::Unordered));
// The outer layer overrides ordering before the concurrent layer validates it.
```

The opposite order would let the concurrent layer validate the caller's original
options before the inner ordering override. Directly passing unordered options
also works. Capabilities are guarantees, not a record of overridden options:
permission to reorder cannot create a preserve-input guarantee.

## Compression

`CompressionLayer::new(policy)` replaces `WriteOptions.compression` for each
session, retaining all other options. It configures the encoder, not an outer byte
compression stream. Enable the requested Parquet codec feature separately; Lance
supports Zstd and Uncompressed; Iceberg accepts global settings only. See
[compression options](../docs/options.md#compression) for complete precedence and
column-name rules.

## Concurrent writes

`UnorderedWriteLayer::new(NonZeroUsize)` reserves one backend session with N
writers and one committer. Explicit unordered permission is required even for
N = 1. A writer receives the next whole nonempty batch when available; no shuffle,
round-robin, or directory scan ordering is promised.

At most one batch per data writer is in flight. When all writers are busy,
`write` waits for capacity. This is a batch bound, not a byte bound; use sensible
batch sizes and account for every backend's encoding buffers. Empty batches are
validated and otherwise ignored. The layer spawns no tasks; pending futures are
polled during write/commit/abort. Native backends may have workers of their own.

For Parquet and Iceberg, row-group encoding and compression run while these
futures are polled in the **same caller task**, even on a multithreaded Tokio
runtime. Several writers can overlap uploads, but the layer does not spread
encoding work across CPU cores. Increasing writer count can help an I/O-bound
workload; for a CPU-bound workload it also multiplies encoding buffers without
adding encoding threads. Lance schedules its native writes in separate tasks,
so its scheduling behavior differs.

Applications requiring CPU parallelism can use `ParallelSink::open_writers`
directly and drive the returned data writers in separate tasks on a multithreaded
executor. This also transfers orchestration responsibilities to the application:
retain writer ownership through failures and cancellation, stop and join every
worker before cleanup, and call the one session committer only after all writers
finish successfully. Returning on the first task error or dropping a join handle
must not leave detached writers running. `WriteSession` itself is an ownership
interface; it does not implement this task scheduler.

A write error or cancellation makes the session terminal. Commit drains accepted
writes and finishes every data writer before passing all outputs to one committer.
A finish failure aborts all writers, including finished ones, before aborting the
session. Abort waits for in-flight writes and attempts every cleanup even if one
fails. Once publication begins, the committer owns uncertain outcomes; the layer
does not delete potentially committed output. Drop performs no async cleanup.

Ordering and compression decorators forward `ParallelSink`. The concurrent sink
itself exposes `Sink`, not `ParallelSink`. `DownloadBlobLayer` also does not expose
`ParallelSink`; placing concurrency outside it will not compile. Placing download
outside concurrency cannot supply native blob methods either. Use blob ingestion
with a supported single writer.

## Download URL columns

```rust,ignore
use std::{num::NonZeroUsize, sync::Arc};
use open_arrow_sink::{SinkExt, lance::LanceDatasetSink,
    layer::{DownloadBlobLayer, DownloadPool}};

let pool = Arc::new(DownloadPool::new(
    NonZeroUsize::new(16).unwrap(),
    NonZeroUsize::new(64 * 1024 * 1024).unwrap(),
)?);
let sink = LanceDatasetSink::default().layer(
    DownloadBlobLayer::new(["image_url".to_owned()].into_iter().collect())
        .with_download_pool(pool),
);
// Open with the URL input schema and Ordering::Unordered.
```

Enable `sink-lance`. Select exact, unique top-level `Utf8`, `LargeUtf8`, or
`Utf8View` columns. The public writer retains the URL input schema; the inner
schema uses blob-marked `LargeBinary` fields. Native handles carry the payloads,
with empty binary placeholders in batches. Nulls, row order within each batch,
batch boundaries, and unrelated metadata are preserved; the final sink's scan
ordering still applies.

The pool streams owned, budgeted chunks directly to the native writer. Payloads
can exceed the transfer-buffer budget; retaining chunks holds their reservations.
Stream concurrency and byte budgets belong to each pool; every constructor
honors its limits. Without a configured pool, each opened writer gets independent
default limits. Share an `Arc<DownloadPool>` when writers should share stream and
transfer budgets. For an HTTP-only budget shared with other pools or storage,
install clones of one `HttpConcurrencyLimitLayer` using `with_layer`. Transfer
budgets exclude SDK/HTTP/encoding buffers. No retry or timeout is installed by
default; OpenDAL I/O policies can be applied to the pool.

The default `DownloadFailurePolicy::Fail` makes a failed write terminal.
For unreliable source URLs, configure `.with_failure_policy(DownloadFailurePolicy::SkipRow)`
to omit a whole row if any selected URL fails, or `DownloadFailurePolicy::Null`
to replace failed blobs with null (selected output fields become nullable).
Payloads still stream directly into native uploads; no whole-blob buffering is added.
Only source-download errors are recoverable, after successful explicit blob abort.
Destination write/finish/cleanup failures and cancellation remain terminal.
The backend must keep a session usable after a healthy explicit blob abort and
clean up completed handles discarded by skipped rows; the Lance backend does both.

Call `layer.metrics()` before applying the layer to retain a `DownloadMetrics`
handle. Its live `snapshot()` reports observed `blobs_failed`, unique
`rows_failed_blobs` within each input batch, and `rows_skipped` from successfully
forwarded batches. Input nulls and destination failures do not count as failed
source downloads. Counters aggregate sessions opened through clones of the layer;
fail-fast/cancellation can stop work before every URL is attempted. For filtered
row counts, place `MetricsLayer` inside `DownloadBlobLayer`; placing it outside
counts accepted input rows.

A failed or cancelled write poisons the session and cancels remaining downloads;
commit then aborts the inner writer. Existing accepted batches may have created
output. Cleanup follows the native writer's guarantees. See the
[complete download contracts](../docs/options.md#streaming-download-pool) and
[Lance blob guide](../sink/lance/README.md#streaming-native-blobs).

To build your own decorator, follow [extend a layer](../docs/extending-layers.md).

## Write metrics

`MetricsLayer` wraps any Sink and forwards ParallelSink/native blob interfaces.
Create a layer, keep its cloneable `metrics()` handle, then install the layer and
inspect `snapshot()` for lifecycle, accepted/committed rows, durations, failures,
and measured backend payload bytes (including unsuccessful sessions). The layer collects observations; backends remain
responsible for measuring real encoded I/O. See [metrics contracts](../docs/options.md#observing-sessions-with-metricslayer).

## Schema evolution

`SchemaEvolutionLayer` performs one-to-one top-level field remapping. It plans
once at open, then applies callbacks to each batch. It never adds, removes, or
reorders columns. All source names refer to the original input schema; renames
can therefore swap names. Missing/ambiguous sources, repeated mappings, and
output name collisions fail before the inner sink opens.

```rust,ignore
use arrow_schema::{DataType, Field};
use open_arrow_sink::{SinkExt, layer::{FieldRemap, SchemaEvolutionLayer}};

let layer = SchemaEvolutionLayer::new(vec![FieldRemap::new(
    "old_name",
    Field::new("new_name", DataType::Int32, true),
    Ok, // Reuse the array for a rename or metadata-only remap.
)]);
let sink = sink.layer(layer);
```

Each mapping supplies the complete target field (including metadata) and a
synchronous `ArrayRef -> Result<ArrayRef>` callback. Use a callback for type or
nested-array conversion; ATIF/Lance policy belongs in the caller. Callbacks must
preserve row count, row order, and relevant nulls/offsets. The layer checks output
length, type, and nullability. Unmapped arrays and schema metadata are preserved.
The public writer exposes the input schema; the backend receives the target
schema. Options, including column names in compression/index policies, refer to
the target schema and pass through unchanged.

Single and parallel sessions are supported. Write errors or cancellation make
the writer terminal. Arbitrary array remaps cannot safely forward native blob
placements, so this adapter does not expose `create_blob`/`write_with_blobs`.
