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
A finish failure returns the error without aborting writers or deleting output.
Explicit abort waits for in-flight writes and stops every writer even if one fails. Once publication begins, the committer owns uncertain outcomes; the layer
does not delete potentially committed output. Drop performs no async cleanup.

Ordering and compression decorators forward `ParallelSink`. The concurrent sink
itself exposes `Sink`, not `ParallelSink`.

## Select URL columns for blob download

Ordinary Arrow strings remain strings. Add `DownloadBlobLayer` to explicitly
select the URL columns whose contents should become blobs:

```rust,ignore
use open_arrow_sink::{SinkExt, lance::LanceDatasetSink, layer::DownloadBlobLayer};

let sink = LanceDatasetSink::default().layer(
    DownloadBlobLayer::new(["image_url".to_owned()].into()),
);
```

Select exact, unique top-level `Utf8`, `LargeUtf8`, or `Utf8View` columns. The layer
marks only those columns as blob inputs, preserving nulls, row counts, other
columns, and metadata. The public writer retains the original input schema.
Single and parallel sessions share the existing schema-transform lifecycle.
Backends without blob input support reject the layer before opening a destination.

Lance's always-present [`BlobDownloader`](../storage/README.md#http-downloads)
performs the downloads under its shared chunk, HTTP, and memory limits. The layer
adds no I/O, second payload buffer, or separate budget. Complete bytes go to the
SDK for inline, packed, or dedicated layout selection. Native logical blob inputs
can still be supplied directly without the layer.

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
