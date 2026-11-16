# Streaming blob extensions

`open-arrow-sink-blob` contains the optional native streaming protocol. Core-only
backends depend on `open-arrow-sink-core` without this crate or `bytes`.
The facade exposes this crate as `open_arrow_sink::blob`.

- `BlobSink::open_blobs` opens a `BlobSessionWriter`, extending the ordinary `Writer` lifecycle.
- `BlobSessionWriter::create_blob` returns a chunk writer; `finish` yields a session-owned handle.
- `write_with_blobs` attaches handles to empty, non-null cells in blob-marked binary columns.
- `BlobChunk` retains download accounting until its payload has been consumed.

`BlobHandle` keeps backend descriptors opaque; backend adapters validate session
ownership and placements. Failed, cancelled, or unfinished streams must prevent
publication. Drop never commits or promises asynchronous cleanup.

Migration: import `BlobChunk`, `BlobHandle`, `BlobPlacement`, and `BlobWriter` from
this crate instead of core. Import `BlobSink` and call `open_blobs` when streaming
is needed; `Sink::open` remains the batch entry point. Lance-specific
`BlobHandleConfig` and `ExternalBlobMode` now live in `open-arrow-sink-lance` and
are set with `LanceDatasetSink::with_blob_handle`, before applying layers.

`DownloadBlobLayer` consumes the extension; `MetricsLayer` forwards it through
its own `open_blobs` entry point. Sinks/layers without this trait cannot be used
as streaming sources. The default `BlobSink::open_blobs` rejects streaming before
opening the destination, allowing adapters that explicitly disable streaming.
