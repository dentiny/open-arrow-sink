# open-arrow-sink

An independent Rust library for writing Apache Arrow `RecordBatch` values to sinks.
It has no dependency on open-arrow-access or any other source library.

The library includes shared contracts, ordering, compression, and unordered write layers, and optional
[Parquet](sink/parquet/README.md), [Iceberg](sink/iceberg/README.md), and
[Lance](sink/lance/README.md) sinks.

```text
core/       Sink, Writer, DataWriter, WriteSession, SessionCommitter, shared options
layer/      OrderingLayer, CompressionLayer, UnorderedWriteLayer
src/        Public facade
```

A `Sink` holds reusable configuration and opens an owned `Writer`. Each backend
chooses its own typed `Request`. The session receives a fixed Arrow schema and
shared `WriteOptions`. Applications supply batches with `write`, then explicitly
`commit` or `abort`. Dropping a writer never commits.

## Ordering

`Ordering::PreserveInput` is the default. It requires preservation of input row
sequence in the backend's documented read order; it does not sort by column values.
A backend declares its guarantee with `SinkCapability { ordering: Ordering::PreserveInput, supported_partitions: vec![], supported_indexes: &[], compression: Default::default() }`
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
Parquet destinations take `url::Url`, including `file://` for local directories.
Enable `parquet-aws`, `parquet-gcp`, or `parquet-azure` for remote object-store
URLs; see the [Parquet sink](sink/parquet/README.md) for configuration and cleanup.

Backend support is declared by each sink; consult its README before selecting
a scope or codec. The shared interface does not promise that every sink supports
every codec.

```rust,ignore
use open_arrow_sink::{Compression, CompressionOptions, SinkExt, layer::CompressionLayer};

let sink = backend.layer(CompressionLayer::new(CompressionOptions {
    global: Some(Compression::Zstd { level: Some(9) }),
    columns: [("embedding".into(), Compression::Uncompressed)].into(),
}));
```

The layer replaces the entire session compression policy and forwards the inner
capability unchanged. Install one compression layer per sink. The same options
can be supplied directly in `WriteOptions`, without a layer.
Column keys are exact, unique top-level Arrow field names: dots are literal,
and a nested field override applies to all its descendant physical columns.
Unknown or ambiguous names are rejected before opening the destination.
Backends retain the session policy across any file rolling.

## Unordered concurrent writes

`UnorderedWriteLayer` is available by default. Unlike `OrderingLayer`,
this layer pre-opens a fixed number of writers and drives their I/O concurrently:

```rust,ignore
use std::num::NonZeroUsize;
use open_arrow_sink::{Ordering, Sink, SinkExt, WriteOptions, layer::UnorderedWriteLayer};

// `backend` implements ParallelSink.
let sink = backend.layer(
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
or `abort`. Backends may use their native workers (Lance uses bounded channels). A successful `write` means accepted; an asynchronous error may surface
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

- `open` calls `validation::validate_options(&schema, &options, capability)` before
  side effects, including index and compression capability checks and compression
  column-name validation. Index-capable backends call `validation::validate_indexes`
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

For concurrent writing, additionally implement `ParallelSink` with a backend-owned
`Output` type. Return exactly the requested number of schema-compatible data writers
and one committer. `DataWriter::finish(&mut self)` returns metadata without publishing;
retain sufficient ownership for `abort` to clean up even after finish. The session
committer receives every output only after all finishes succeed. Its `abort` runs
after all writer aborts, including when a writer abort reports an error. Once its
`commit` is entered, it owns publication and cleanup decisions; the layer will not
abort the data writers afterward. Drop/cancelled open or commit do not promise async
cleanup. Metadata accumulates until commit; the batch limit is not a metadata limit.

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
