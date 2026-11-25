# Extending

Use a sink for destination-specific encoding and publication. Use a layer for
policy or transformations shared across destinations. Depend on
`open-arrow-sink-core`; no registration or changes to core are required.

## Add a sink

Start with the runnable [counting sink](../layer/examples/support/counting_sink.rs)
and its [driver](../layer/examples/custom_sink.rs). It demonstrates the contracts
without storage or service credentials; replace its row counter with your encoder
or SDK. For a persistent backend, see [Parquet](../sink/parquet/src/sink.rs) and
its [committer](../sink/parquet/src/committer.rs). Reuse
[`DestinationResolver`](../storage/src/destination.rs) for explicit or URL-derived
OpenDAL destinations: it validates retry/timeout configuration and shares cached
backends and HTTP budgets across clones. Apply session options with
`destination_resolver.resolve(&url)?.with_write_options(&options)`.

1. Implement [`Sink`](../core/src/sink.rs). Choose a typed `Request` and advertise
   only capabilities you support. Validate shared options with
   `validation::validate_options`, then backend parameters and destination state
   before writing output. Index-capable sinks also call `validate_indexes`.
2. Implement [`DataWriter`](../core/src/session.rs). Validate each batch against
   the fixed schema and keep buffering bounded. `finish(&mut self)` closes the
   physical output and returns backend-owned metadata without publishing table
   or dataset state. The writer remains abortable after finish.
3. Implement [`SessionCommitter<Output>`](../core/src/session.rs). Publish all
   finished outputs once and return `Result<CommitSummary>` with actual published
   row counts aggregated across all outputs. Publication and visibility follow
   your backend's semantics; document them.
4. Assemble a `WriteSession` with one writer and use synchronous `into_writer()` for
   ordinary `Sink::open`. For concurrent writes, implement `ParallelSink`:
   reserve the destination once, return exactly the requested number of writers
   with disjoint output ownership, and share one committer.

Use [`Poison`](../core/src/poison.rs) before fallible or awaited write work;
call `complete` only after success. Errors and cancellation must prevent later
writes or commit. Finishing is terminal. `abort` stops I/O and retains persisted
output; drop performs no async cleanup. A failed commit may already have
published data, so never delete potentially referenced files automatically.

A specialized backend can implement [`Writer`](../core/src/writer.rs) directly,
but must preserve the same lifecycle. Advertise native blob support only when
accepting logical blob values through ordinary batch writes; see
[Lance's SDK adapter](../sink/lance/src/blob.rs). Pass logical blob bytes to
the backend SDK and let it select inline, packed, or dedicated storage. Do not
write blob objects or construct physical blob descriptors in this library.

`open-arrow-sink:blob="bytes"` marks `Binary`, `LargeBinary` and `BinaryView` inputs as payload bytes.
The Lance adapter normalizes them to its logical LargeBinary data field,
preserving nulls, slices and arbitrary non-UTF-8 bytes. This also lets source
readers retain zero-copy BinaryView buffers until the write boundary.

`open-arrow-sink:blob="uri"` marks `Utf8`, `LargeUtf8` or `Utf8View` source URIs.
The default policy downloads them; `ExternalBlobMode::Reference` retains external
references. Explicit markers must match the Arrow type or schema validation fails.
Legacy `"true"` remains supported and infers bytes versus URI from the field type.

`DownloadBlobLayer` marks selected URL fields with `"uri"` for blob ingestion, using the same
interface for top-level columns and dotted paths through structs and lists. The backend
owns downloads, resource limits, and blob layout; the layer does not download
or buffer payloads.

For a bundled backend, add the workspace member, optional facade dependency,
`sink-<name>` feature, and gated export in [src/lib.rs](../src/lib.rs). Add a
write/readback example and a row to the root README's backend table.

## Add a layer

Start with [custom_layer.rs](../layer/examples/custom_layer.rs), which implements
a maximum batch size policy and verifies that rejected writes cannot commit.

1. Implement [`Layer<S>`](../core/src/layer.rs). Construct a wrapping sink without
   I/O and preserve `S::Request`. Validate layer configuration before opening
   the inner sink; forward unrelated options and validation.
2. Wrap the writer. Forward schema, consuming commit/abort, and any advertised
   blob methods. Use [`GuardedWriter`](../core/src/guard.rs) (or
   `GuardedDataWriter` for parallel writers) to preserve error and cancellation
   handling. A schema transform retains both schemas: expose the input schema
   and open the inner writer with the transformed schema.
3. Preserve only capabilities your layer actually retains. `ParallelSink` is
   not forwarded automatically: option layers can delegate it, while batch
   transforms must wrap every returned data writer to avoid bypassing policy.

The last installed layer is outermost: `sink.layer(a).layer(b)` sends operations
through `b → a → sink`. State composition constraints and resource bounds.
Generic layers leave backend outputs and publication to the sink. See
[CompressionLayer](../layer/src/compression.rs) for option forwarding and
[DownloadBlobLayer](../layer/src/download_blob.rs) for a schema transformation.

For a bundled layer, export it from [layer/src/lib.rs](../layer/src/lib.rs) and
add a runnable example.

## Verify an extension

Run the reference examples without external services:

```sh
cargo run --locked -p open-arrow-sink-layer --example custom_sink
cargo run --locked -p open-arrow-sink-layer --example custom_layer
```

For a workspace backend, implement
[`SinkFixture`](../test-support/src/conformance.rs) and run
`assert_conforms::<YourFixture>().await`. It covers round trips, empty commits,
abort, and schema-error termination. Keep backend-specific checks beside the
backend: supported options and modes, cancellation, parallel publication, and
retention of output after failures. Layer tests should cover schema handling,
poisoning, capability forwarding, and supported composition orders.
