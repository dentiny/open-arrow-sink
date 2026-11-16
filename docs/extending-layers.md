# Extend a layer

Implement a layer for a policy or transformation reusable across sinks: batch
validation, metrics, schema adaptation, or execution strategy. The layer preserves
the inner sink's request type and leaves backend publication/cleanup to that sink.
For a new destination, follow [extend a sink](extending-sinks.md).

## Start with a runnable implementation

[custom_layer.rs](../layer/examples/custom_layer.rs) implements `MaxBatchRows`,
a wrapping sink, and a wrapping writer. It rejects oversized batches, forwards
accepted batches and native blob methods, and aborts a poisoned session instead
of committing earlier accepted rows.

```sh
cargo run --locked -p open-arrow-sink-layer --example custom_layer
```

The driver checks a successful three-row session and a failed session whose
previously accepted row must remain unpublished. Its in-memory counting sink
makes the example runnable without a storage service.

## 1. Construct a wrapping sink

Implement `Layer<S: Sink>` with `type Layered = YourSink<S>`. `layer(&self, inner)`
only stores configuration and the inner sink; it performs no I/O. The returned
sink implements `Sink<Request = S::Request>`. Import `SinkExt` at the call site:

```rust,ignore
let sink = backend.layer(MaxBatchRows(1024));
```

`capability()` should preserve only guarantees your wrapper actually retains. An
option-override layer forwards capabilities because it changes policy, not what
the backend can guarantee. A reordering execution layer must advertise Unordered.
If the wrapper forwards `supports_blob_streaming`, it must forward/implement
the blob writer methods too; inheriting their unsupported defaults would contradict capabilities.

## 2. Validate and open the inner session

Reject invalid layer configuration before opening the inner sink. If overriding
options, modify them before validation/open. Keep the backend's remaining options
intact, including modes, compression, partitions, indexes, versions, and cleanup.
Validate only layer-owned constraints; the inner sink performs shared and
backend-specific validation after all option overrides.

For a schema transform, compute the output schema at open and retain both schemas.
Expose the caller's schema from the outer writer; give transformed batches only
to the inner writer opened with the transformed schema. Preserve metadata unless
the transform intentionally changes it. Validate exact input schema on every
write, including empty batches.

## 3. Wrap the writer without weakening its lifecycle

| Method | Required behavior |
| --- | --- |
| `schema()` | Fixed caller-facing schema |
| `write(batch)` | Check outer state/schema, transform or validate, await inner acceptance |
| `create_blob()` / `write_with_blobs(...)` | Forward or adapt if advertising support; validate batch policy on both write paths |
| `commit(self: Box<Self>)` | Abort a poisoned inner session; otherwise consume and delegate commit exactly once |
| `abort(self: Box<Self>)` | Consume and delegate abort, releasing layer-owned resources |

Return `Box::new(GuardedWriter::new(Box::new(your_writer)))` from `open`.
The core guard owns terminal-state transitions around the entire decorator,
including validation/transform errors before the inner writer is called. It
rejects writes after an error or cancellation and aborts a poisoned session on
commit. The decorator itself only implements its policy and cleanup delegation;
it does not need its own `Poison` field. See the batch-limit example above.

A cancelled write cannot invoke async cleanup; retain enough ownership for a
later explicit abort. Do not turn errors into successful empty batches or retry
a potentially committed session. Resource `Option`s may still be needed to move
ownership into finishing or cleanup; they are not a second usability flag.

If the layer creates tasks, buffers, or streams, document bounds and ensure abort
stops and awaits owned work. A batch-count bound is not a byte-count bound. Never
publish backend state or interpret opaque data-writer outputs in a generic layer.

## 4. Decide whether to preserve ParallelSink

Implementing `Sink` alone does **not** forward `ParallelSink`. The batch-limit
example intentionally exposes only `Sink`, so it can wrap an already-concurrent
sink but cannot sit inside `UnorderedWriteLayer`.

An option-only layer can additionally implement `ParallelSink` for inner
`S: ParallelSink`, using `type Output = S::Output`, applying its options, and
forwarding `open_writers`. See [OrderingLayer](../layer/src/ordering.rs) and
[CompressionLayer](../layer/src/compression.rs).

A per-batch transformation must also wrap every returned `DataWriter` if it
exposes `ParallelSink`; merely forwarding `open_writers` would bypass that policy.
Wrap each transformed writer with `GuardedDataWriter::new` to enforce terminal
writes and one-shot finish while retaining abort ownership. Give independent
parallel writers independent guards; finishing one must not disable the others.
Preserve the requested writer count, schemas, output ownership, and single
committer. Blob transformation currently does not support the concurrent path;
[DownloadBlobLayer](../layer/src/download_blob.rs) is a useful schema-transform
reference with that restriction.

## 5. Document composition and integrate

`sink.layer(a).layer(b)` sends open/write through b → a → sink. For example, an
ordering override must be outside concurrency if it is meant to enable unordered
execution before validation. State what your layer replaces, what guarantees it
retains, whether it forwards parallel/blob support, and where it can sit in a stack.

Extensions can live out of tree. For a built-in layer, add a module/export in
`layer/src/lib.rs`, dependencies/features only where required, a guide entry in
[layer/README.md](../layer/README.md), and a runnable example. The facade already
re-exports the layer crate. Verify error/cancellation poisoning, commit/abort
forwarding, schema handling, capability accuracy, and both supported composition
orders. See [development](development.md) for validation commands.
