# open-arrow-sink

An independent Rust library for writing Apache Arrow `RecordBatch` values to sinks.
It has no dependency on open-arrow-access or any other source library.

The library includes shared contracts, ordering and unordered write layers, and optional
[Parquet](sink/parquet/README.md), [Iceberg](sink/iceberg/README.md), and
[Lance](sink/lance/README.md) sinks.

```text
core/       Sink, Writer, Layer, options, capabilities, validation and errors
layer/      OrderingLayer and UnorderedWriteLayer
src/        Public facade
```

A `Sink` holds reusable configuration and opens an owned `Writer`. Each backend
chooses its own typed `Request`. The session receives a fixed Arrow schema and
shared `WriteOptions`. Applications supply batches with `write`, then explicitly
`commit` or `abort`. Dropping a writer never commits.

## Ordering

`Ordering::PreserveInput` is the default. It requires preservation of input row
sequence in the backend's documented read order; it does not sort by column values.
A backend declares its guarantee with `SinkCapability { ordering: Ordering::PreserveInput }`
or `Ordering::Unordered` (no input-order guarantee). It must reject a preserve-input
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
before opening it. Install one ordering layer per sink. Layers wrap without I/O;
the last installed layer is outermost.

## Unordered concurrent writes

`UnorderedWriteLayer` is available by default. Unlike `OrderingLayer`,
this layer pre-opens a fixed number of writers and drives their I/O concurrently:

```rust,ignore
use std::num::NonZeroUsize;
use open_arrow_sink::{Ordering, Sink, SinkExt, WriteOptions, layer::UnorderedWriteLayer};
use open_arrow_sink::parquet::ParquetDirectorySink; // feature `sink-parquet`

let sink = ParquetDirectorySink.layer(
    UnorderedWriteLayer::new(NonZeroUsize::new(4).unwrap()),
);
let mut writer = sink.open(directory, schema, WriteOptions {
    ordering: Ordering::Unordered,
    target_file_size: Some(256 * 1024 * 1024),
    ..WriteOptions::default()
}).await?;
writer.write(batch).await?;
let summary = writer.commit().await?;
```

The inner sink must implement `ParallelSink`; Parquet supports this today. The
trait lets a backend reserve one destination and assign disjoint output files
without cloning requests or opening the same directory repeatedly. Iceberg and
Lance do not yet implement this trait. No source-library metadata is needed.

At most one batch per writer is in flight. When all writers are busy, `write`
waits for one to finish. An available writer accepts the next nonempty batch;
there is no round-robin or file-order guarantee. Empty batches are validated and
otherwise ignored. The bound is in batches, not bytes. Each backend also retains
its own encoding buffers.

There are no spawned tasks: I/O progresses while `write`, `commit`, or `abort`
is polled. A successful `write` means accepted; an asynchronous error may surface
in a later call. An error or cancellation of `write` makes the session terminal;
abort or drop it. Commit drains writes before committing each backend writer and
sums their row counts. Abort waits for in-flight I/O and attempts to abort every
writer, even if another abort fails. Drop never commits or performs async cleanup.

`Ordering::PreserveInput` is rejected before opening files. An outer
`OrderingLayer::new(Ordering::Unordered)` can explicitly enable reordering.

Commit is not atomic across files. If one writer fails to commit, remaining
writers are aborted, but already committed files may remain visible. Cleanup
honors `cleanup_on_error` and never removes another writer's or unrelated files.

## File size

The shared `WriteOptions.target_file_size` option specifies a positive byte count:

```rust
use open_arrow_sink::{Ordering, WriteOptions};

let options = WriteOptions {
    ordering: Ordering::Unordered,
    target_file_size: Some(256 * 1024 * 1024),
    ..WriteOptions::default()
};
```

This is a soft data-file target, not a strict maximum or a total-output limit.
Files can exceed it because batches, pages, row groups, and footers are not split
at exact byte boundaries. `None` preserves the backend default, and zero is
rejected before opening a destination. Backend-specific rolling is implemented
separately; see each sink's README for support and granularity. Other backend
limits may also cause rolling before the target is reached.

## Implementing a sink

Implement `Sink` and `Writer` from the facade or `open-arrow-sink-core`:

- `open` calls `validation::validate_options` before side effects, then checks
  backend-specific destination constraints.
- `write` calls `validation::validate_batch` against the fixed session schema.
  Equality includes column order, types, nullability and metadata; casts are explicit.
- `commit` completes buffered writes using the backend's documented visibility
  semantics. The common interface does not promise atomic publication.
- `abort` follows `WriteOptions.cleanup_on_error` (enabled by default). Cleanup is best
  effort and cannot promise rollback of visible data.

After a write error or cancellation, abort or drop the writer. No automatic retry,
write modes, schema evolution or transaction framework is included in this change.

## Development

```sh
cargo fmt --all --check
cargo test --workspace --locked
cargo clippy --workspace --all-targets --locked -- -D warnings
```
