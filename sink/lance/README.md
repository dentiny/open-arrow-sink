# Lance dataset sink

`LanceDatasetSink` creates a new local Lance dataset from Arrow batches. It requires
a Tokio runtime and a new UTF-8 directory path whose parent already exists.
Existing paths are rejected. Keep exclusive ownership of the destination until the
session ends.

```rust,ignore
use open_arrow_sink::{Ordering, Sink, WriteOptions, lance::LanceDatasetSink};

let mut writer = LanceDatasetSink.open(
    "./new.lance".into(),
    schema,
    WriteOptions { ordering: Ordering::Unordered, ..WriteOptions::default() },
).await?;
writer.write(batch).await?;
let summary = writer.commit().await?;
```

Enable the facade's `sink-lance` feature, or depend on `open-arrow-sink-lance`
directly. Cloud backends in Lance are disabled.

`WriteOptions.target_file_size` sets Lance's native `max_bytes_per_file` parameter.
`None` keeps the SDK default. It is a soft data-file target: buffered pages, groups,
and file footers can make files larger; Lance's row-count limits also still apply.

Writes feed a capacity-one queue into Lance's native streaming writer. Native
buffering is bounded by SDK defaults; this adapter never collects all input
batches. `write` means accepted by the queue; encoding errors may surface on a
later write or commit. Only `commit` sends a successful end-of-stream and allows
Lance to publish its first manifest. `abort` stops the worker and removes the
reserved directory. Dropping closes the stream with an error and never commits,
but can leave orphan files for manual cleanup. Commit failures/cancellation can
have an uncertain publication outcome; do not automatically retry.

This first version supports create-only local datasets and unordered input.
No append, overwrite, indexes, schema evolution, cloud options or ordering guarantee.
The destination may contain uncommitted files before commit, but becomes readable
as a Lance dataset only when the native manifest is published.

A manual write/read example (destination must not already exist):

```sh
cargo run -p open-arrow-sink-lance --example write_lance -- /tmp/new-example.lance
```

The native Lance dependency needs `protoc` available when building from source.

## Downloaded blobs

Compose this sink with `DownloadBlobLayer` (facade feature `layer-download-blob`)
to download URL columns before writing. The layer produces `LargeBinary` fields
marked with `open-arrow-sink:blob = true`; this sink converts those fields to native
Lance Blob v2 (`ARROW:extension:name = lance.blob.v2`) with the payload in `data`,
using the SDK's default 2.2 file format. Nulls and empty payloads remain distinct.
Unmarked binary fields remain ordinary binary columns. Applications may also
supply marked `LargeBinary` fields directly; invalid marked types are rejected
before creating the destination.

The writer reports and validates the input schema. The stored dataset schema
contains native blob structs, so it intentionally differs for these columns.
Lance's default scans return blob descriptors; use
`dataset.scan().blob_handling(lance::datatypes::BlobHandling::AllBinary)` to
materialize payloads as `LargeBinary`, or use Lance's blob read APIs for lazy reads.
