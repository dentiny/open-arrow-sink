# Extend a sink

Implement a sink when adding a destination with its own encoding, reservation,
publication, or cleanup semantics. Use a layer for a reusable policy around an
existing destination. Extensions can live in an application or separate crate;
no registration system or changes to core are required.

## Start with a runnable implementation

The [counting sink](../layer/examples/support/counting_sink.rs) is a complete,
non-persistent implementation of `Sink`, `ParallelSink`, `DataWriter`, and
`SessionCommitter`. It counts accepted rows privately and exposes the count only
when the session commits. It deliberately stores no Arrow payloads and provides
no durability; replace that physical writer with your encoder/SDK.

```sh
cargo run --locked -p open-arrow-sink-layer --example custom_sink
```

The [driver](../layer/examples/custom_sink.rs) composes two writers, checks that
nothing is published before commit, and verifies one combined six-row summary.
It lives in the layer crate to reuse its Tokio test runtime; backend authors can
depend on `open-arrow-sink-core` alone plus their own I/O dependencies.

## 1. Define the request and capabilities

`Sink: Send + Sync` is reusable configuration. Put destination-specific arguments
in `type Request: Send`: URLs for file/dataset sinks, a catalog identifier for a
table sink, or a typed struct for your service. Do not require source-library
metadata or assume where batches originated.

Return a complete `SinkCapability`: supported modes, actual read-order guarantee,
global/per-column codecs, partition transform categories, index kinds, blob
configuration support (`supports_blob_handle`), streaming blob interfaces
(`supports_blob_streaming`), and storage-version support. Empty codec/index/partition collections mean
unsupported. A permission to reorder is not a guarantee of preserved order.
Directory file numbering alone cannot establish scan order.

Capabilities cannot express every request restriction. Document and validate
those restrictions at open, such as filesystem-only overwrite or a schema that
requires a specialized blob writer. Reject options you cannot honor; the example
rejects file-size targets because it has no files.

## 2. Validate before opening output

Inside `open`/`open_writers`, before destination I/O:

1. Call `validation::validate_options(&schema, &options, self.capability())`.
2. For index support, call `validation::validate_indexes`, then validate native
   index column types. Validate codec levels, storage version syntax, blob
   configuration, and all other backend-only parameters.
3. Resolve/load the destination, then check existence, write mode, exact schema,
   native partition spec, and other destination-dependent constraints.
4. Reserve output once. Record ownership so cleanup cannot remove another
   session's data. If opening multiple writers fails partway, abort every opened
   writer and clean up the session according to `cleanup_on_error`.

Use `unique_column_index` for exact unique top-level names; dots are literal.
An unknown or ambiguous column must not silently select another field.

## 3. Implement physical writing

Implement `DataWriter<Output = YourOutput>` with these responsibilities:

| Method | Contract |
| --- | --- |
| `schema()` | Original fixed input schema, even if internal encoding adds metadata |
| `write(batch)` | Validate the exact schema; bounded buffering; errors/cancellation make this writer terminal |
| `finish(&mut self)` | Called once after writes complete; close encoding, return owned metadata, do not publish table/dataset state |
| `abort(self: Box<Self>)` | Stop I/O and best-effort cleanup of owned output, including after successful finish |

Use `Poison::begin` **before** any fallible or awaited write work and call
`complete` only after a successful write. Errors and cancelled futures then leave
the writer terminal and subsequent operations return `Error::Poisoned`. Call
`begin` at finish too, without `complete`, so a finished writer stays terminal.
The counting example demonstrates this pattern.
Real backends also need to retain upload/worker ownership after cancellation.

Use `StorageWriter` or your native SDK for streaming. Honor `target_file_size` as
a positive soft target and retain session compression policy across rolling.
Advertise `supports_upload_options` only when both session upload overrides are
honored. Shared-storage backends can apply them to a clone with
`storage.clone().with_write_options(&options)` before opening writers; this keeps
other sessions' defaults unchanged. Unsupported explicit overrides are rejected
by shared validation.
Keep row buffering bounded and document non-row growth such as file metadata,
index training, or one writer per partition. Wrap native errors in
`Error::backend(error)` to retain the source.

## 4. Implement publication separately

`SessionCommitter<YourOutput>` receives all finished outputs exactly once. It
owns native commit, the combined `WriteSummary`, and publication-failure cleanup.
Before publication, abort can remove known owned output. After publication begins,
an error or cancellation can leave the result uncertain: do not delete files
that might already be referenced by a table or manifest. Document how callers
reconcile the destination before retrying.

`SessionCommitter::abort` runs after every data writer has been aborted, even if
an individual abort failed; clean up remaining session resources too. Never
publish on drop or rely on drop for async cleanup.

## 5. Expose ordinary and parallel entry points

Build a `WriteSession { writers, committer }`. For ordinary `Sink::open`, return
exactly one data writer and adapt it using `session.into_writer().await`. The
adapter handles poisoned writes, finish errors, and pre-publication aborts.
A backend can implement `Writer` directly for a specialized lifecycle, but must
provide all equivalent failure, cancellation, consuming commit/abort, and optional
blob-method behavior itself.

For `ParallelSink::open_writers`, return **exactly** the requested nonzero count,
all with the requested schema, disjoint output names/ownership, and one committer.
The layer treats your `Output` as opaque metadata and cannot merge manifests or
infer cleanup rules for you.

```mermaid
flowchart LR
    Open["Reserve destination once"] --> A["DataWriter A"]
    Open --> B["DataWriter B"]
    A --> OA["finish: Output A"]
    B --> OB["finish: Output B"]
    OA --> Commit["SessionCommitter: one publication"]
    OB --> Commit
```

Adding native blobs also requires `Writer::create_blob` and `write_with_blobs`.
Keep `BlobChunk` accounting alive until bytes are consumed; return session-owned
handles only after the blob finishes. Reject foreign/duplicate placements,
nonempty or null placeholder cells, and invalid blob columns. An unfinished or
failed blob poisons its session. See the [blob contracts](../core/src/blob.rs) and
[Lance implementation](../sink/lance/src/blob_writer.rs).

## 6. Integrate and verify

For an in-workspace backend, add `sink/<name>` to workspace members, use the shared
package metadata/version, add an optional dependency and `sink-<name>` facade
feature, and gate the facade module export. Forward cloud/codec features where
applicable. Add a backend README, a write/read example, and support-matrix entry.

Verify the behaviors users depend on: rejected options produce no output;
readback matches row count and values; schema mismatches and cancelled writes
cannot later commit; abort preserves unrelated and pre-existing data; finished
writers still clean up after another finish fails; publication occurs once; and
ambiguous commits retain potentially referenced files. Exercise every advertised
mode, compression scope, empty session, partition/index/blob restriction, and
parallel path your implementation supports.

Reference implementations:
[Parquet sink](../sink/parquet/src/sink.rs) and
[committer](../sink/parquet/src/committer.rs),
[Iceberg sink](../sink/iceberg/src/sink.rs) and
[committer](../sink/iceberg/src/committer.rs),
[Lance sink](../sink/lance/src/sink.rs) and
[committer](../sink/lance/src/committer.rs).

For workspace backends, implement the dev-only
[`SinkFixture`](../test-support/src/conformance.rs) and run
`assert_conforms::<YourFixture>().await` against a fresh factory. The shared suite
checks round trips, readable empty commits, abort, and schema-error termination.
It deliberately leaves storage-specific publication and cleanup assertions in
the fixture/backend tests. `core/tests/session.rs` independently exercises the
adapter's cancellation and publication boundaries without a backend.
