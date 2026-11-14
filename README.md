# open-arrow-sink

An independent Rust library for writing Apache Arrow `RecordBatch` values to sinks.
It has no dependency on open-arrow-access or any other source library.

The library includes shared contracts, an ordering layer, an optional rebatch layer, and optional
[Parquet](sink/parquet/README.md), [Iceberg](sink/iceberg/README.md), and
[Lance](sink/lance/README.md) sinks.

```text
core/       Sink, Writer, Layer, options, capabilities, validation and errors
layer/      OrderingLayer, optional RebatchLayer
src/        Public facade
```

A `Sink` holds reusable configuration and opens an owned `Writer`. Each backend
chooses its own typed `Request`. The session receives a fixed Arrow schema and
shared `WriteOptions`. Applications supply batches with `write`, then explicitly
`commit` or `abort`. Dropping a writer never commits.

## Ordering

`Ordering::PreserveInput` is the default. It requires preservation of input row
sequence in the backend's documented read order; it does not sort by column values.
A backend declares its guarantee with `SinkCapability { ordering: Ordering::PreserveInput, supported_partitions: vec![] }`
or an `Ordering::Unordered` ordering capability (no input-order guarantee). It must reject a preserve-input
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

## Rebatching

Enable the facade feature `layer-rebatch` to use `RebatchLayer`:

```rust,ignore
use std::num::NonZeroUsize;
use open_arrow_sink::{SinkExt, layer::RebatchLayer};

let sink = backend.layer(RebatchLayer::new(NonZeroUsize::new(64_000).unwrap()));
```

The layer merges small batches and splits large batches while preserving row order.
Each forwarded batch has the configured row count, except the final partial batch
sent by `commit`. Empty batches are ignored after schema validation. `abort` drops
pending rows and aborts the inner writer. No background tasks or concurrency are added.

Pending slices contain at most the configured number of rows. Arrow slices may
retain their input buffers, so this is a row bound, not a byte-memory limit.

## Partitioning

`WriteOptions.partition: Some(PartitionOptions::default())` enables routing using
an existing destination's partition spec. `None` disables routing (the default).
Only the Iceberg sink currently supports this option, for identity partition
transforms; Parquet and Lance reject it before creating output. Backends declare
supported transforms through `SinkCapability.supported_partitions`: Iceberg lists
`PartitionType::Identity`, and an empty list means partitioning is unsupported. No partition layer is required.

`PartitionOptions.max_open_writers` is a positive limit on distinct partitions
per session (default 128). Exceeding it fails the write; abort or drop the session.
It bounds active writer count, not bytes. See the [Iceberg sink](sink/iceberg/README.md)
for configuration and commit semantics.

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
