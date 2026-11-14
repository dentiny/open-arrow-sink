# open-arrow-sink

An independent Rust library for writing Apache Arrow `RecordBatch` values to sinks.
It has no dependency on open-arrow-access or any other source library.

The library includes shared contracts, ordering, optional rebatch and blob download
layers, and optional
[Parquet](sink/parquet/README.md), [Iceberg](sink/iceberg/README.md), and
[Lance](sink/lance/README.md) sinks.

```text
core/       Sink, Writer, Layer, options, capabilities, validation and errors
layer/      OrderingLayer, optional RebatchLayer and DownloadBlobLayer
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

## Downloading URL columns as blobs

Enable `layer-download-blob` to use `DownloadBlobLayer`. Select top-level URL
columns explicitly using a `HashSet<String>`; each selected column is replaced in
place with `LargeBinary`
containing the downloaded HTTP(S) response body. `Utf8`, `LargeUtf8`, and `Utf8View`
inputs are supported. Nulls remain null without a request; empty files remain
non-null empty binary values. Other columns and row order are preserved.

```rust,ignore
use std::collections::HashSet;
use open_arrow_sink::{SinkExt, layer::DownloadBlobLayer, lance::LanceDatasetSink};

// Enable both `layer-download-blob` and `sink-lance`.
let url_columns: HashSet<String> = ["image_url", "audio_url"]
    .map(str::to_owned).into_iter().collect();
let sink = LanceDatasetSink.layer(DownloadBlobLayer::new(url_columns));
// Open with the original URL schema and Ordering::Unordered for Lance.
// writer.schema() continues to expose the original input schema.
```

The layer marks output fields with `open-arrow-sink:blob = true`. The Lance sink
maps these fields to native **Blob v2** columns and writes the downloaded bytes
into Lance-owned storage. Other sinks receive ordinary `LargeBinary` values with
the marker. Ordinary unmarked binary columns retain their existing behavior.

Downloads run sequentially on a Tokio runtime, following HTTP redirects, with no
retries. Defaults are a 30-second total request timeout, a 16 MiB limit per blob,
and at most 16 rows per forwarded batch. Configure these with `with_timeout`,
`with_max_blob_bytes`, and `with_rows_per_batch` (the latter two take
`NonZeroUsize`). The size limit is checked while reading, including responses
without a content length. Buffering scales with output rows × selected columns ×
maximum blob size, plus a response and Arrow allocation overhead; slices can retain
input buffers. An inner rebatch layer can add its own buffering.

A failed or cancelled write poisons the session: subsequent writes fail and
`commit` aborts the inner writer. Earlier output chunks may already have been
accepted; cleanup follows the backend's guarantees. Missing, ambiguous, or
non-string selected columns are rejected before the inner sink opens. Source field metadata is retained except for Arrow extension-type metadata, which no
longer describes the converted binary column. Only explicit top-level columns and
HTTP(S) URLs are supported in this version.

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
