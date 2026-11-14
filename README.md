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
A backend declares its guarantee with `SinkCapability { ordering: Ordering::PreserveInput, supported_partitions: vec![], supported_indexes: &[] }`
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
`PartitionType::Identity`, and an empty list means partitioning is unsupported.
The requested `PartitionOptions.partition_type` (default `Identity`) must be
in that list. No partition layer is required.

The Iceberg adapter uses the SDK's fanout writer behavior. See the
[Iceberg sink](sink/iceberg/README.md) for configuration and commit semantics.

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

## Index construction

Index requests are backend-independent `IndexSpec` and `IndexType` values in
`WriteOptions.indexes`. The request list is empty by default.
`SinkCapability.supported_indexes` lists the supported `IndexType` values. Each
requested type must appear in that list. Lance supports Scalar, Vector, Text,
and BloomFilter; Parquet and Iceberg expose empty lists and reject index requests
before opening a writer or accessing the destination.
General layers forward this capability and the options to their inner sink.

```rust
use open_arrow_sink::{IndexType, IndexSpec, Ordering, WriteOptions};

let options = WriteOptions {
    ordering: Ordering::Unordered,
    indexes: vec![
        IndexSpec::new("id", IndexType::Scalar),
        IndexSpec::new("embedding", IndexType::Vector),
    ],
    ..WriteOptions::default()
};
```

Backends choose the concrete implementation of each logical index type and validate
column types before writing. Lance maps the four kinds to BTree, IVF_FLAT with L2,
Inverted, and BloomFilter, then constructs them after committing the dataset's data.
See the [Lance documentation](sink/lance/README.md) for construction failure semantics.
`WriteOptions` owns its index list and implements `Clone`; clone options when
reusing them across sessions.

## Implementing a sink

Implement `Sink` and `Writer` from the facade or `open-arrow-sink-core`:

- `open` calls `validation::validate_options` before side effects, including the
  index capability check. Index-capable backends call `validation::validate_indexes`
  and check their supported column types before destination I/O, then check
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

Run the checks below locally to lint Rust code and Cargo manifests. Install Taplo with
`cargo install taplo-cli --version 0.10.0 --locked`. Clippy also requires `protoc`
for the Lance dependencies (`protobuf-compiler` on Ubuntu).

```sh
cargo fmt --all --check
taplo fmt --check
taplo lint --no-schema
cargo metadata --no-deps --locked --format-version 1 > /dev/null
cargo test --workspace --locked
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
```

Run `cargo fmt --all` and `taplo fmt` to apply formatting fixes.
