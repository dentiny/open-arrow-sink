# open-arrow-sink

An independent Rust library for writing Apache Arrow `RecordBatch` values to sinks.
It has no dependency on open-arrow-access or any other source library.

This initial foundation contains only shared contracts and an ordering layer.
Parquet directory, Iceberg table and Lance dataset implementations will follow
under `sink/` in separate changes.

```text
core/       Sink, Writer, Layer, options, capabilities, validation and errors
layer/      OrderingLayer
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

## Implementing a sink

Implement `Sink` and `Writer` from the facade or `open-arrow-sink-core`:

- `open` calls `validation::validate_options` before side effects, then checks
  backend-specific destination constraints.
- `write` calls `validation::validate_batch` against the fixed session schema.
  Equality includes column order, types, nullability and metadata; casts are explicit.
- `commit` completes buffered writes using the backend's documented visibility
  semantics. The common interface does not promise atomic publication.
- `abort` performs best-effort cleanup and cannot promise rollback of visible data.

After a write error or cancellation, abort or drop the writer. No automatic retry,
write modes, schema evolution or transaction framework is included in this change.
The integration test in `layer/tests/ordering.rs` shows a minimal custom backend.

## Development

```sh
cargo fmt --all --check
cargo test --workspace --locked
cargo clippy --workspace --all-targets --locked -- -D warnings
```
