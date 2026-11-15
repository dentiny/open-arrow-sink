# Parquet directory sink

`ParquetDirectorySink` accepts a `url::Url` for a new directory or object-store
prefix and writes uncompressed files named `part-00000.parquet`, `part-00001.parquet`, etc. By default it writes
one file per session. For `file://` URLs the parent must exist; an existing target
is rejected, including an empty directory. Remote destinations require a nonempty,
unused prefix. Enable facade feature `sink-parquet`
and import `open_arrow_sink::parquet::ParquetDirectorySink`, or use this crate directly.


## Destination URLs

The request type for both `Sink::open` and `ParallelSink::open_writers` is now
`Url` (re-exported from this crate). Convert existing local paths with
`Url::from_directory_path(std::path::absolute(path)?)`; relative paths must first
be made absolute. URL escaping is decoded by the backend, so spaces and Unicode
in local paths and remote keys are supported. Queries and fragments are rejected.

```rust,ignore
use open_arrow_sink_parquet::{ParquetDirectorySink, Url};
use open_arrow_sink_core::{Ordering, Sink, WriteOptions};

let destination: Url = "s3://my-bucket/new-output/".parse()?;
let mut writer = ParquetDirectorySink.open(destination, schema, WriteOptions {
    ordering: Ordering::Unordered,
    ..Default::default()
}).await?;
writer.write(batch).await?;
writer.commit().await?;
```

Cloud backends use `object_store` and are opt-in:

| URL | Sink crate feature | Facade feature |
| --- | --- | --- |
| `file:///absolute/path` | Always available | `sink-parquet` |
| `s3://bucket/prefix` | `aws` | `parquet-aws` |
| `gs://bucket/prefix` | `gcp` | `parquet-gcp` |
| `az://container/prefix` | `azure` | `parquet-azure` |

Backend configuration is read from environment variables understood by
`object_store`, such as `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`,
`AWS_REGION`, and `AWS_ENDPOINT`. Unsupported schemes and disabled backends
return errors; URLs are never interpreted as local filesystem paths.
`memory:///prefix` is also available for ephemeral writes; each open creates a
fresh in-memory store.

Remote sessions reserve a prefix by conditionally creating a hidden
`.open-arrow-sink` object. This marker remains on success and must be excluded
from readers that scan all objects (select `*.parquet`). Existing objects at or
below the prefix cause open to fail. Conditional creation also reserves each
part name before multipart upload. Concurrent sessions using this sink cannot
share a prefix. External writers must not modify a reserved prefix.

Remote files stream through multipart uploads with a 5 MiB buffer and one part
in flight per writer, in addition to Parquet encoding buffers. Reserved part
names may be visible as empty objects until the upload finishes. Rolled files
become readable before session commit; there is no atomic prefix transaction.
Abort with cleanup enabled aborts active uploads and deletes only owned parts;
the marker is removed when the prefix is otherwise empty. Cleanup disabled or
dropping a writer can leave incomplete multipart uploads, which may require a
bucket lifecycle rule or explicit backend cleanup.

Use `WriteOptions { ordering: Ordering::Unordered, ..Default::default() }`: this
sink does not promise directory scan order. Each write validates its schema and flushes its rows; it
does not retain all input batches. Parquet still keeps row-group metadata and
needs memory proportional to the incoming batch and encoding buffers.

Set `WriteOptions.target_file_size` to a positive byte count to roll files between
nonempty batches once the current file's encoded bytes reach the target. A batch
and the file footer can exceed the target; it is not a strict maximum. `None`
keeps the single-file behavior. An empty session still creates one readable file.

Commit completes the final file footer. The target is visible while writing and has no
atomic directory transaction or durability/fsync guarantee.

`WriteOptions.cleanup_on_error` defaults to `true`: abort removes this session's files and
attempts to remove its directory; failed open/commit also attempts cleanup. Set it
to `false` to retain session-created output on abort or failure. Cleanup never
recursively deletes other files. Drop leaves incomplete output and never commits.

No append, overwrite, partitioning or sink-level retries.

For concurrent files, wrap the sink in
`UnorderedWriteLayer::new(NonZeroUsize::new(4).unwrap())`. The layer calls
`ParallelSink::open_writers` to reserve the directory once and pre-open four
files. Each writer rolls independently using disjoint indices (writer 0 owns
0, 4, 8, ...; writer 1 owns 1, 5, 9, ...). The target size applies to each file.
Empty sessions produce one readable empty file per pre-opened writer. Batch
distribution and directory scan order are unspecified. Data writers finish all
footers before the session committer returns the combined row count. A finish
failure aborts every writer according to the cleanup policy. Files remain visible
before commit; there is no atomic directory transaction. See the root README for
buffering, cancellation, partial commit, and layer composition semantics.

```sh
cargo run -p open-arrow-sink-parquet --example write_directory -- file:///tmp/new-parquet-directory
```

## Index requests

This sink declares `SinkCapability.supported_indexes = &[]`. A nonempty
`WriteOptions.indexes` list returns `Error::Unsupported` before opening a writer
or accessing the destination. The default empty list keeps existing behavior.

## Compression

Supports global compression and per-column overrides through `WriteOptions.compression`
or `CompressionLayer`. Uncompressed is always available. Other codecs require
opt-in Cargo features (none are enabled by default):

| Codec | Sink crate feature | Facade feature |
| --- | --- | --- |
| Snappy | `snappy` | `parquet-snappy` |
| Gzip | `gzip` | `parquet-gzip` |
| Zstd | `zstd` | `parquet-zstd` |
| Lz4Raw | `lz4` | `parquet-lz4` |

For example, enable `parquet-zstd` on `open-arrow-sink`, or `zstd` on
`open-arrow-sink-parquet`. Facade codec features also enable `sink-parquet`.
Capabilities advertise only enabled codecs; disabled codec requests return
`Error::Unsupported` before creating the destination. Cargo features are additive,
so other dependencies may still bring in codecs through their own Parquet features.

Column keys are exact top-level Arrow names; nested fields apply to all descendant columns.
Overrides and global settings persist across file rolling.

Explicit Zstandard levels use `Compression::Zstd { level: Some(level) }`.
Parquet accepts levels 1 through 22; unsupported levels are rejected before
creating the destination. `level: None` uses the Parquet default.
